package io.hyperfoil.tools.horreum.exp.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.horreum.api.exp.data.Label;
import io.hyperfoil.tools.horreum.api.exp.LabelService;
import io.hyperfoil.tools.horreum.exp.data.*;
import io.hyperfoil.tools.horreum.exp.mapper.LabelMapper;
import io.hyperfoil.tools.horreum.hibernate.JsonBinaryType;
import io.hyperfoil.tools.horreum.server.WithRoles;
import io.hyperfoil.tools.horreum.svc.Roles;
import io.hyperfoil.tools.horreum.svc.ServiceException;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.POST;
import org.hibernate.query.NativeQuery;

import java.util.*;
import java.util.function.Function;

@ApplicationScoped
public class LabelServiceImpl implements LabelService {

    private static final String COUNT_TEST_BY_ID_QUERY = "SELECT count(id) FROM exp_test WHERE id = ?1";

    @Transactional
    @WithRoles
    @RolesAllowed(Roles.VIEWER)
    public Label get(long id){
        return LabelMapper.from(LabelDAO.findById(id));
    }

    //cannot be transactional and cannot be run in a transaction as it must write each to db as it runs
    public void calculateLabelValues(Collection<LabelDAO> labels, Long runId){
        for(LabelDAO l : labels){
            calculateLabelValue(l,runId);
        }
    }

    public record ExtractedValue(long sourceValueId, long souceLabelId, boolean isIterated, int ordinal, JsonNode data){

        @Override
        public String toString(){
            return "EV valueId="+sourceValueId+" labelId="+souceLabelId+" ordinal="+ordinal+" iterated="+isIterated+" data="+data;
        }

        public boolean hasSourceValue(){
            return souceLabelId > 0;
        }
    }
    public static class ExtractedValues {
        private final Map<String,Long> extractorLabelSources = new HashMap<>();
        private final Map<String,Map<Long,Set<Integer>>> extractorOrdinals = new HashMap<>();
        private final Map<String,List<ExtractedValue>> byName = new HashMap<>();

        public void add(String name,long valueId,long labelId, boolean iterated,int ordinal, JsonNode data){
            if(data == null || data.isNull()){
                return;
            }
            if (!byName.containsKey(name)) {
                byName.put(name,new ArrayList<>());
            }
            if(!extractorOrdinals.containsKey(name)){
                extractorOrdinals.put(name,new HashMap<>());
            }
            if(!extractorOrdinals.get(name).containsKey(valueId)){
                extractorOrdinals.get(name).put(valueId,new HashSet<>());
            }
            if (!extractorOrdinals.get(name).get(valueId).contains(ordinal)) {
                extractorOrdinals.get(name).get(valueId).add(ordinal);
                ExtractedValue v = new ExtractedValue(valueId,labelId,iterated,ordinal,data);
                extractorLabelSources.put(name,labelId);
                byName.get(name).add(v);
            }else{
                //conflict for the ordinal + name
                ExtractedValue v = new ExtractedValue(valueId,labelId,iterated,ordinal,data);
                //byName.get(name).remove(v.ordinal);//ordinal is not guaranteed to match the index
                //this linear scan is not ideal but a more efficient algorithm is needed for most of this PoC
                byName.get(name).replaceAll(ev->{
                    if(ev.ordinal == ordinal){
                        return v;
                    }else{
                        return ev;
                    }
                });
            }

        }
        public boolean hasNonNull(String name){
            return byName.containsKey(name) && !byName.get(name).isEmpty();
        }

        public List<Map<String,ExtractedValue>> getLengthGrouped(){
            List<Map<String,ExtractedValue>> rtrn = new ArrayList<>();
            Map<String,Integer> nameIterators = new HashMap<>();
            for(String name: byName.keySet()){
                nameIterators.put(name,0);
            }
            while(nameIterators.entrySet().stream().anyMatch(e->e.getValue() < byName.get(e.getKey()).size())){

                Map<String,ExtractedValue> next = new HashMap<>();

                Map<Long,Long>  labelIdtoValueId = new HashMap<>(); //track which label we are targeting
                //sort the names by iteratorValue
                List<String> sortedNames = new ArrayList<>(byName.keySet());
                Function<String,Integer> getScore = (n)->{
                    if(byName.get(n).size() > nameIterators.get(n)){
                        return byName.get(n).get(nameIterators.get(n)).ordinal;
                    }else{
                        return Integer.MAX_VALUE;
                    }
                };
                sortedNames.sort(Comparator.comparingInt(getScore::apply));
                for(String name : sortedNames){

                    int currentIndex = nameIterators.get(name);
                    if(currentIndex < byName.get(name).size()) {
                        ExtractedValue nextValue = byName.get(name).get(currentIndex);
                        //this doesn't work. it needs to get the lowest sourceValueId across the
                        if (!labelIdtoValueId.containsKey(nextValue.souceLabelId)) {
                            labelIdtoValueId.put(nextValue.souceLabelId, nextValue.sourceValueId);
                        }
                        if (nextValue.sourceValueId == labelIdtoValueId.get(nextValue.souceLabelId)) {
                            //this means the current extracted value is from the same label source with the same value source
                            next.put(name, nextValue);
                            nameIterators.put(name, 1 + currentIndex);
                        }else{
                            //
                        }
                    }
                }
                if(next.isEmpty()){
                    //hmm.... how did this happen
                }else{
                    rtrn.add(next);
                }
            }
            return rtrn;
        }

        public List<ExtractedValue> getByName(String name){
            return byName.getOrDefault(name,Collections.emptyList());
        }
        public boolean sameSource(String name,String otherName){
            return Objects.equals(extractorLabelSources.getOrDefault(name, -1L), extractorLabelSources.getOrDefault(otherName, 1L));
        }
        public int size(){return byName.size();}
        public Set<String> getNames(){return byName.keySet();}
        //        public ObjectNode asNode(){
//            ObjectNode rtrn = JsonNodeFactory.instance.objectNode();
//            for(String name : getNames()){
//                List<ExtractedValue> extractedValues = getByName(name);
//                if(extractedValues.size()==0){
//                    rtrn.set(name,JsonNodeFactory.instance.nullNode());
//                }else if(extractedValues.size()==1){
//                    rtrn.set(name,extractedValues.get(0).data);
//                }else{
//                    ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
//                    for(ExtractedValue v : extractedValues){
//                        arrayNode.add(v.data);
//                    }
//                    rtrn.set(name,arrayNode);
//                }
//            }
//            return rtrn;
//        }
        public String toString(){
            StringBuilder sb = new StringBuilder();
            for(String name : getNames()){
                sb.append("name="+name+" "+ getByName(name).size()+"\n");
                for( ExtractedValue v : getByName(name)){
                    sb.append("  source="+v.sourceValueId+" labelId="+v.souceLabelId+" iter="+v.isIterated+" data="+v.data+"\n");
                }
            }
            return sb.toString();
        }
    }

    @Transactional
    public void calculateLabelValue(LabelDAO l, Long runId){
        ExtractedValues extractedValues = calculateExtractedValuesWithIterated(l,runId);
        RunDao r = RunDao.findById(runId);
        if(l.extractors.size()==1){
            //we do not have to deal with multitype if there is only one extractor
            List<ExtractedValue> evs = extractedValues.getByName(l.extractors.get(0).name);
            for(ExtractedValue ev : evs){
                if(ev.isIterated){
                    if(ev.data.isArray()){//I think this should always be true
                        ArrayNode arrayNode = (ArrayNode) ev.data;
                        int idx = 0;
                        for(JsonNode childNode : arrayNode){
                            LabelValueDao newValue = new LabelValueDao();
                            newValue.ordinal=idx;
                            idx++;
                            newValue.data = childNode;
                            if(l.reducer!=null){
                                newValue.data = l.reducer.evalJavascript(newValue.data);
                            }
                            newValue.label = l;
                            newValue.run = r;
                            if(ev.sourceValueId() > 0) {

                                LabelValueDao referenced = LabelValueDao.findById(ev.sourceValueId);
                                newValue.sources.add(referenced);
                            }else{
                                //it doesn't have a source, this is ok
                            }
                            newValue.persist();
                        }
                    }else{
                        //this means an error occurred in calculating
                    }
                }else{
                    //
                    LabelValueDao newValue = new LabelValueDao();
                    newValue.ordinal=0;
                    newValue.data = ev.data;
                    if(l.reducer!=null){
                        newValue.data = l.reducer.evalJavascript(newValue.data);
                    }
                    newValue.label = l;
                    newValue.run = r;
                    newValue.persist();
                    if(ev.sourceValueId > 0){
                        //this can happen when the value is derived from a previous label but not iterated
                        LabelValueDao referenced = LabelValueDao.findById(ev.sourceValueId);
                        newValue.sources.add(referenced);
                    }
                    newValue.persist();
                }
            }
        } else {
            //this is the challenging bit where we have to deal with NxN et.al.
            List<Map<String,ExtractedValue>> todo = extractedValues.getLengthGrouped();
            for(Map<String,ExtractedValue> map : todo){
                //if any of them are iterated we have to

                boolean haveIterated = map.values().stream().anyMatch(ev->ev.isIterated);
                if(!haveIterated) {
                    //create the object node
                    ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
                    LabelValueDao newValue = new LabelValueDao();
                    newValue.ordinal = 0;
                    newValue.label = l;
                    newValue.run = r;
                    map.forEach((k, v) -> {
                        objectNode.set(k, v.data);
                        //TODO what if the sourceLabelId is invalid?
                        if(v.hasSourceValue()) {
                            newValue.addSource(LabelValueDao.findById(v.sourceValueId));
                        }
                    });
                    if(l.reducer!=null){
                        newValue.data = l.reducer.evalJavascript(objectNode);
                    }else{
                        newValue.data = objectNode;
                    }
                    newValue.persistAndFlush();
                }else{
                    //TODO this needs to calculate the all the combinations of iterated extracted values and use the scalar
                    switch (l.multiType){
                        case Length -> {
                            int maxLength = l.extractors.stream()
                                    .filter(e -> ExtractorDao.Type.VALUE.equals(e.type) && (e.targetLabel.hasForEach() || e.forEach) && map.containsKey(e.name) && map.get(e.name).data != null )
                                    .map(e->map.get(e.name).data.isArray() ? map.get(e.name).data.size() : 1)
                                    .max(Integer::compareTo).orElse(1);
                            for(int i=0; i<maxLength; i++){
                                ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
                                LabelValueDao newValue = new LabelValueDao();
                                newValue.ordinal=i;
                                newValue.label=l;
                                newValue.run=r;
                                for(ExtractorDao e : l.extractors){
                                    if(map.containsKey(e.name)){
                                        ExtractedValue v = map.get(e.name);
                                        //size = 0 for scalar objects so we need to account for them separately
                                        if(!v.data().isArray()){
                                            if(i == 0 ){
                                                objectNode.set(e.name,v.data);
                                            }else{

                                            }

                                        }else if( (e.forEach || v.isIterated) && v.data.size() > i){
                                            objectNode.set(e.name,v.data.get(i));

                                        }else if (!e.forEach && (Label.ScalarVariableMethod.All.equals(l.scalarMethod) || i == 0) ){
                                            objectNode.set(e.name,v.data);
                                        }

                                        if(objectNode.has(e.name) && v.hasSourceValue()) {
                                            newValue.addSource(LabelValueDao.findById(v.sourceValueId));
                                        }
                                    }
                                }
                                if(l.reducer!=null){
                                    newValue.data = l.reducer.evalJavascript(objectNode);
                                }else{
                                    newValue.data = objectNode;
                                }
                                newValue.persistAndFlush();
                            }
                        }
                        case NxN -> {
                            //TODO implement NxN for iterated labelValues
                            System.err.println("NxN not yet implemented");
                        }

                    }
                }

            }
        }
    }

    @Inject
    EntityManager em;

    @POST
    @Transactional
    public long add(LabelDAO label){
        if(label.id!=null && label.id == -1){
            label.id = null;
        }
        em.persist(label);
        em.flush();
        return label.id;
    }

    /*
     * Get all the LabelValues for the given test that target the specific schema.
     */
    public List<LabelValueDao> getBySchema(String schema,Long testId){
        return LabelValueDao.find("from LabelValueDao lv where lv.label.target_schema = ?1 and lv.label.parent.id = ?2",schema,testId).list();
    }


    public List<LabelDAO> getDescendantLabels(Long labelId){
        List<LabelDAO> rtrn = new ArrayList<>();

        //noinspection unchecked
        LabelDAO.getEntityManager().createNativeQuery(
                        """
                            with recursive bag(id) as ( values(:labelId) union select l.id from bag b,exp_extractor e left join exp_label l on e.parent_id = l.id where e.target_id =b.id) select * from bag
                            """
                ).setParameter("labelId",labelId)
                .unwrap(NativeQuery.class)
                .addScalar("id",Long.class)
                .list()
                .forEach(v->{
                    Long id = (Long)v;
                    LabelDAO found = LabelDAO.getEntityManager().getReference(LabelDAO.class,id);
                    rtrn.add(found);
                });
        return rtrn;
    }

    /*
     * Gets the LabelValues that reference the given index in parent labelValue
     */
    public List<LabelValueDao> getDerivedValues(LabelValueDao parent,int index){
        //return LabelValue.find("from LabelValue lv where exists (from LabelValuePointer lvp where lvp.child = lv and lvp.target=?1 and lvp.targetIndex = ?2)",parent,index).list();
        return LabelValueDao.find("SELECT LV FROM LabelValueDao LV, IN (LV.sources) S WHERE S.id = ?1",parent.id).list();
    }

    /**
     * Checks whether the provided id belongs to an existing test and if the user can access it
     * the security check is performed by triggering the RLS at database level
     * @param id test ID
     */
    @WithRoles
    @Transactional
    protected boolean checkTestExists(long id) {
        return 0 != (Long) em.createNativeQuery(COUNT_TEST_BY_ID_QUERY, Long.class)
              .setParameter(1, id)
              .getSingleResult();
    }

    //get the labelValues for all instances of a target schema for a test
    //could also have a labelValues based on label name, would that be useful? label name would not be merge-able across multiple labels
    public List<LabelService.ValueMap> labelValues(String schema,long testId, List<String> include, List<String> exclude){
        List<ValueMap> rtrn = new ArrayList<>();
        String labelNameFilter = "";
        if (include!=null && !include.isEmpty()){
            if(exclude!=null && !exclude.isEmpty()){
                include = new ArrayList<>(include);
                include.removeAll(exclude);
            }
            if(!include.isEmpty()) {
                labelNameFilter = " WHERE bag.name in :include";
            }
        }
        //includeExcludeSql is empty if include did not contain entries after exclude removal
        if(labelNameFilter.isEmpty() && exclude!=null && !exclude.isEmpty()){
            labelNameFilter=" WHERE bag.name NOT in :exclude";
        }
        NativeQuery query = (NativeQuery) em.createNativeQuery(
                        """
                        with recursive bag(run_id,value_id,source_id,name,data,parent_id) as
                        (
                            select
                                lv.run_id as run_id,
                                lvs.LabelValueDao_id as value_id,
                                lvs.sources_id as source_id,
                                l.name as name,
                                lv.data as data,
                                lvs.sources_id as parent_id
                            from
                                exp_label_value_sources lvs
                                left join exp_label_values lv on lvs.LabelValueDao_id = lv.id
                                left join exp_label l on lv.label_id = l.id
                            where
                                lvs.sources_id in (select lv.id from exp_label_values lv left join exp_label l on l.id = lv.label_id where l.target_schema = :schema and l.parent_id = :testId)
                            union all
                            select
                                bag.run_id as run_id,
                                lvs.LabelValueDao_id as value_id,
                                lvs.sources_id as source_id,
                                l.name as name,
                                lv.data as data,
                                bag.parent_id as parent_id
                            from
                                exp_label_value_sources lvs
                                left join exp_label_values lv on lvs.LabelValueDao_id = lv.id
                                left join exp_label l on lv.label_id = l.id
                                join bag on lvs.sources_id = bag.value_id
                        ),
                        grouped as
                        (
                            select
                                run_id,
                                parent_id,
                                source_id,
                                name,
                                jsonb_agg(data) as data
                            from
                                bag
                            LABEL_NAME_FILTER
                            group by run_id,parent_id,source_id,name order by parent_id,source_id
                        ),
                        stack as
                        (
                            select
                                run_id,
                                parent_id,
                                name,
                                jsonb_agg((case when jsonb_array_length(data) > 1 then data else data->0 end)) as data
                            from grouped group by run_id,parent_id,name
                        ) select run_id,parent_id,name,data from stack
                        """.replace("LABEL_NAME_FILTER",labelNameFilter)
                ).setParameter("schema",schema)
                .setParameter("testId",testId);

        if(!labelNameFilter.isEmpty()){
            if(labelNameFilter.contains("include")){
                query.setParameter("include",include);
            }
            if(labelNameFilter.contains("exclude")){
                query.setParameter("exclude",exclude);
            }
        }

        //noinspection unchecked
        List<Object[]> found = query
                .unwrap(NativeQuery.class)
                .addScalar("run_id",Long.class)
                .addScalar("parent_id",Long.class)
                .addScalar("name",String.class)
                .addScalar("data", JsonBinaryType.INSTANCE)
                .list();

        Map<Long, LabelService.ValueMap> maps = new HashMap<>();

        for(Object[] object: found){
            Long runId = (Long)object[0];
            Long parentId = (Long)object[1];
            String name = (String)object[2];
            JsonNode data = (JsonNode)object[3];

            if(!maps.containsKey(parentId)){
                LabelService.ValueMap entry = new LabelService.ValueMap(JsonNodeFactory.instance.objectNode(),parentId,parentId,runId,testId);
                maps.put(parentId,entry);
            }
            LabelService.ValueMap entry = maps.get(parentId);
            if(data.isArray()){
                if(((ArrayNode)data).size()>1){
                    entry.data().set(name,data);
                }else{
                    entry.data().set(name,((ArrayNode)data).get(0));
                }
            }else {
                entry.data().set(name,data);
            }
        }
        return maps.values().stream().sorted((a,b)->Long.compare(a.runId(),b.runId())).toList();
    }
    //This is the labelValues endpoint that more closely matches what currently exists in Horreum if run = dataset
    //filter,before,after,sort,direction,limit,page, and multiFilter are not yet supported
    List<LabelService.ValueMap> labelValues(
            long  testId,
            String filter,
            String before,
            String after,
            String sort,
            String direction,
            int limit,
            int page,
            List<String> include,
            List<String> exclude,
            boolean multiFilter){
        if (!checkTestExists(testId)) {
            throw ServiceException.serverError("Cannot find test "+testId);
        }
        
        List<LabelService.ValueMap> rtrn = new ArrayList<>();
        String labelNameFilter = "";
        if (include!=null && !include.isEmpty()){
            if(exclude!=null && !exclude.isEmpty()){
                include = new ArrayList<>(include);
                include.removeAll(exclude);
            }
            if(!include.isEmpty()) {
                labelNameFilter = " AND l.name in :include";
            }
        }
        //includeExcludeSql is empty if include did not contain entries after exclude removal
        if(labelNameFilter.isEmpty() && exclude!=null && !exclude.isEmpty()){
            labelNameFilter=" AND l.name NOT in :exclude";
        }

        //noinspection rawtypes
        NativeQuery query = (NativeQuery) em.createNativeQuery(
                        """
                        with bag as (
                            select
                                r.test_id, lv.run_id, l.name,
                                jsonb_agg(lv.data) as data
                            from exp_label_values lv
                                left join exp_label l on l.id = lv.label_id
                                left join exp_run r on r.id = lv.run_id
                            where r.test_id = :testId
                            LABEL_NAME_FILTER
                            group by r.test_id,lv.run_id,l.name
                        )
                        select
                            run_id, test_id,
                            jsonb_object_agg(name,(case when jsonb_array_length(data) > 1 then data else data->0 end)) as data
                        from bag
                        group by test_id,run_id;
                        """.replace("LABEL_NAME_FILTER",labelNameFilter)
                )
                .setParameter("testId",testId);
        if(!labelNameFilter.isEmpty()){
            if(labelNameFilter.contains("include")){
                query.setParameter("include",include);
            }
            if(labelNameFilter.contains("exclude")){
                query.setParameter("exclude",exclude);
            }
        }

        //noinspection unchecked
        List<Object[]> found = query
                .unwrap(NativeQuery.class)
                .addScalar("run_id",Long.class)
                .addScalar("test_id",Long.class)
                .addScalar("data",JsonBinaryType.INSTANCE)
                .getResultList();
        
        for(Object[] object : found){
            // tuple (labelId,index) should uniquely identify which label_value entry "owns" the ValueMap for the given test and run
            // note a label_value can have multiple values that are associated with a (labelId,index) if it is NxN
            Long runId = (Long)object[0];
            //object[1] is testId
            ObjectNode data = (ObjectNode)object[2];

            LabelService.ValueMap vm = new LabelService.ValueMap(data,-1,-1,runId,testId);
            rtrn.add(vm);
        }
        return rtrn;
    }

    public LabelValueDao getLabelValue(long runId, long labelId){
        return LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",runId,labelId).firstResult();
    }

    private void debug(String sql,Object...args){
        List<Object> found;
        NativeQuery q = LabelDAO.getEntityManager().createNativeQuery(sql).unwrap(NativeQuery.class);
        for(int i=0; i<args.length; i++){
            q.setParameter(i+1,args[i]);
        }
        found = q.getResultList();
        if(found!=null){
            found.forEach(row->{
                if(row == null){
                    //
                }else{
                    if(row instanceof Object[]){
                        System.out.printf("%s%n",Arrays.asList((Object[])row).toString());
                    }else {
                        System.out.printf("%s%n",row.toString());
                    }
                }
            });
        }
    }

    /*
        LabelValueExtractor on an iterated label_value will need to run N separate times because it will be forced to be an iterated value
     */
    public ExtractedValues calculateExtractedValuesWithIterated(LabelDAO l, long runId){
        ExtractedValues rtrn = new ExtractedValues();

        //debugging again
        //a for-each that isn't iterated...?
        //when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.foreach and jsonb_typeof(m.lv_data) = 'array' then extract_path_array(m.lv_data,m.jsonpath::jsonpath)

        //do we need to check jsonb_typeof
        //right now this assumes we don't get garbage data... probably not a safe assumption
        //unchecked is how you know the code is great :)
        @SuppressWarnings("unchecked")
        List<Object[]> found = LabelDAO.getEntityManager().createNativeQuery("""
            with m as (
                select
                    e.name, e.type, e.jsonpath, e.foreach, e.column_name,
                    lv.id as value_id, lv.label_id as label_id, lv.data as lv_data, lv.ordinal as ordinal,
                    r.data as run_data, r.metadata as run_metadata
                from
                    exp_extractor e full join exp_label_values lv on e.target_id = lv.label_id,
                    exp_run r where e.parent_id = :label_id and (lv.run_id = :run_id or lv.run_id is null) and r.id = :run_id),
            n as (select m.name, m.type, m.jsonpath, m.foreach, m.value_id, m.label_id, m.ordinal, (case
                when m.type = 'PATH' and m.jsonpath is not null then jsonb_path_query_array(m.run_data,m.jsonpath::jsonpath)
                when m.type = 'METADATA' and m.jsonpath is not null and m.column_name = 'metadata' then jsonb_path_query_array(m.run_metadata,m.jsonpath::jsonpath)
                when m.type = 'VALUE' and m.jsonpath is not null and m.jsonpath != '' then jsonb_path_query_array(m.lv_data,m.jsonpath::jsonpath)
                when m.type = 'VALUE' and (m.jsonpath is null or m.jsonpath = '') then to_jsonb(ARRAY[m.lv_data])
                else '[]'::jsonb end) as found from m)
            select n.name as name,n.value_id, n.label_id, n.ordinal, (case when jsonb_array_length(n.found) > 1 or strpos(n.jsonpath,'[*]') > 0 then n.found else n.found->0 end) as data, n.foreach as lv_iterated from n
            order by label_id,value_id
        """).setParameter("run_id",runId).setParameter("label_id",l.id)
                //TODO add logging in else '[]'
                .unwrap(NativeQuery.class)
                .addScalar("name",String.class)
                .addScalar("value_id",Long.class)
                .addScalar("label_id",Long.class)
                .addScalar("ordinal",Integer.class)
                .addScalar("data", JsonBinaryType.INSTANCE)
                .addScalar("lv_iterated",Boolean.class)
                .getResultList();
        if(found.isEmpty()){
            //TODO alert error or assume the data missed all the labels?
        }else {
            for(int i=0; i<found.size(); i++){
                Object[] row = (Object[])found.get(i);
                String name = (String)row[0];
                Long valueId = (Long) row[1];
                Long labelId = (Long) row[2];
                Integer ordinal = (Integer) row[3];
                JsonNode data = (JsonNode) row[4];
                Boolean iterated = (Boolean)row[5];
                rtrn.add(name,valueId == null ? -1: valueId ,labelId == null ? -1 : labelId,iterated != null && iterated,ordinal == null ? 0 : ordinal,data);
            }
        }
        return rtrn;
    }


}
