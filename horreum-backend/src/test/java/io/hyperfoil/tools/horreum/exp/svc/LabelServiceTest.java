package io.hyperfoil.tools.horreum.exp.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.horreum.api.exp.LabelService;
import io.hyperfoil.tools.horreum.api.exp.data.Label;
import io.hyperfoil.tools.horreum.api.exp.data.Test;
import io.hyperfoil.tools.horreum.exp.data.*;
import io.hyperfoil.tools.horreum.test.HorreumTestProfile;
import io.hyperfoil.tools.horreum.test.PostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Disabled;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(PostgresResource.class)
@QuarkusTestResource(OidcWiremockTestResource.class)
@TestProfile(HorreumTestProfile.class)
public class LabelServiceTest {
    @Inject
    EntityManager em;
    @Inject
    TransactionManager tm;
    @Inject
    LabelServiceImpl labelService;

    @Inject
    Validator validator;


    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_JsonPathExtractor() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        t.loadLabels(a1);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,a1.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals(a1Node,lv.data);
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_no_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");

        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO found = new LabelDAO("found",t)
                .loadExtractors(ExtractorDao.fromString("a1").setName("found"));
        t.loadLabels(a1,found);

        Set<ConstraintViolation<TestDao>> violations = validator.validate(t);

        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();


        assertNotNull(lv,"label_value should exit");
        assertEquals(a1Node,lv.data);
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");

        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO found = new LabelDAO("found",t)
                .loadExtractors(ExtractorDao.fromString("a1:$[0].key").setName("found"));
        t.loadLabels(a1,found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals("a1_alpha",lv.data.asText());
    }
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_forEach_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");

        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO found = new LabelDAO("found",t)
                .loadExtractors(ExtractorDao.fromString("a1[]:$.key").setName("found"));
        t.loadLabels(a1,found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();
        List<LabelValueDao> lvs = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).list();
        assertNotNull(lvs,"label_value should exit");
        assertEquals(3,lvs.size(),lvs.toString());
    }

    //case when m.dtype = 'JsonpathExtractor' and m.jsonpath is not null
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_JsonpathExtractor_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        t.loadLabels(a1);
        t.persist();
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": \"found\", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);
        LabelServiceImpl.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(a1,r.id);

        assertTrue(extractedValues.hasNonNull(a1.name),extractedValues.toString());
        assertFalse(extractedValues.getByName(a1.name).get(0).isIterated());
        assertInstanceOf(List.class,extractedValues.getByName(a1.name));
        List<LabelServiceImpl.ExtractedValue> values = extractedValues.getByName(a1.name);
        assertEquals(1,values.size());
        assertEquals("found",values.get(0).data().asText());
    }
    //case when m.dtype = 'RunMetadataExtractor' and m.jsonpath is not null and m.column_name = 'metadata'
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_RunMetadataExtractor_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO jenkinsBuild = new LabelDAO("build",t)
                .loadExtractors(ExtractorDao.fromString(
                        ExtractorDao.METADATA_PREFIX+"metadata"+ ExtractorDao.METADATA_SUFFIX+
                                ExtractorDao.NAME_SEPARATOR+ ExtractorDao.PREFIX+".jenkins.build").setName("build")
                );
        t.loadLabels(jenkinsBuild);
        t.persist();
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": \"found\", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);
        LabelServiceImpl.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(jenkinsBuild,r.id);
        assertTrue(extractedValues.hasNonNull(jenkinsBuild.name));
        assertFalse(extractedValues.getByName(jenkinsBuild.name).get(0).isIterated());
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.getByName(jenkinsBuild.name).get(0).data());
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.foreach and jsonb_typeof(m.lv_data) = 'array'


    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.lv_iterated
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_iterated_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO iterA = new LabelDAO("iterA",t)
                .loadExtractors(ExtractorDao.fromString("a1[]").setName("iterA"));
        LabelDAO foundA = new LabelDAO("foundA",t)
                .loadExtractors(ExtractorDao.fromString("iterA:$.key").setName("foundA"));
        t.loadLabels(foundA,iterA,a1);
        t.persist();
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        LabelServiceImpl.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(foundA,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(foundA.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.getByName(foundA.name).get(0).isIterated());
        assertEquals(3,extractedValues.getByName(foundA.name).size(),"unexpected number of entries in "+extractedValues.getByName(foundA.name));
    }
    //case when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != ''
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO firstAKey = new LabelDAO("firstAKey",t)
                .loadExtractors(ExtractorDao.fromString("a1:$[0].key").setName("firstAKey"));
        t.loadLabels(firstAKey,a1);
        t.persist();
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);
        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);

        LabelServiceImpl.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(firstAKey,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(firstAKey.name),"missing extracted value\n"+extractedValues);
        assertFalse(extractedValues.getByName(firstAKey.name).get(0).isIterated());
        //It's a text node because it is quoted in the json
        assertInstanceOf(TextNode.class,extractedValues.getByName(firstAKey.name).get(0).data(),"unexpected: "+extractedValues.getByName(firstAKey.name));
        assertEquals("a1_alpha",((TextNode)extractedValues.getByName(firstAKey.name).get(0).data()).asText());
    }
    //case when m.dtype = 'LabelValueExtractor' and (m.jsonpath is null or m.jsonpath = '')
    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateExtractedValuesWithIterated_LabelValueExtractor_no_jsonpath() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO iterA = new LabelDAO("iterA",t)
                .loadExtractors(ExtractorDao.fromString("a1[]").setName("iterA"));
        t.loadLabels(iterA,a1);
        t.persist();
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": [ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}], \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        //must call calcualteLabelValues to have the label_value available for the extractor
        labelService.calculateLabelValues(t.labels,r.id);
        LabelDAO l = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","iterA",t.id).firstResult();
        LabelServiceImpl.ExtractedValues extractedValues = labelService.calculateExtractedValuesWithIterated(l,r.id);
        assertEquals(1,extractedValues.size(),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.hasNonNull(l.name),"missing extracted value\n"+extractedValues);
        assertTrue(extractedValues.getByName(l.name).get(0).isIterated());
        assertEquals(1,extractedValues.getByName(l.name).size(),"unexpected number of entries in "+extractedValues.getByName(l.name));
        assertInstanceOf(ArrayNode.class,extractedValues.getByName(l.name).get(0).data(),"unexpected: "+extractedValues.getByName(l.name));
        assertEquals(3,extractedValues.getByName(l.name).get(0).data().size(),"unexpected number of entries in "+extractedValues.getByName(l.name)+"[0]");
    }
    @org.junit.jupiter.api.Test
    public void createLabelValues_nested() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, JsonProcessingException, NotSupportedException {
        tm.begin();
        TestDao t = new TestDao("nested");
        LabelDAO foo = new LabelDAO("fo",t)
                .loadExtractors(ExtractorDao.fromString("$.foo").setName("fo"));
        LabelDAO iterFoo = new LabelDAO("foo",t)
                .loadExtractors(ExtractorDao.fromString("fo[]").setName("foo"))
                .setTargetSchema("foos");
        LabelDAO bar = new LabelDAO("bar",t)
                .loadExtractors(ExtractorDao.fromString("foo[]:$.bar").setName("bar"));
        LabelDAO biz = new LabelDAO("biz",t)
                .loadExtractors(ExtractorDao.fromString("bar[]:$.biz").setName("biz"));
        LabelDAO sum = new LabelDAO("sum",t)
                .loadExtractors(
                        ExtractorDao.fromString("biz[]:$.a").setName("a"),
                        ExtractorDao.fromString("biz[]:$.b").setName("b")
                ).setReducer("({a,b})=>(a||'')+(b||'')");
        t.loadLabels(foo,iterFoo,bar,biz,sum);
        t.persistAndFlush();
        RunDao r = new RunDao(
                t.id,
                new ObjectMapper().readTree("""
                {
                    "foo": [
                      {
                        "bar": [
                          {
                            "biz": [
                              { "a": "a00", "b": "b00"},
                              { "a": "a01"},
                              { "a": "a02", "b": "b02"}
                            ]
                          }
                        ]
                      },
                      {
                        "bar": [
                          {
                            "biz": [
                              { "a": "a10", "b": "b10"},
                              { "b": "b11"},
                              { "a": "a12", "b": "b12"}
                            ]
                          }
                        ]
                      }
                    ]
                }
                """),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValueDao> lvs = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,sum.id).list();

        assertEquals(6,lvs.size(),"expect 6 values for sum: "+lvs);
        Arrays.asList("a00b00","a01","a02b02","a10b10","b11","a12b12").forEach(v->{
            assertTrue(lvs.stream().anyMatch(lv->lv.data.isTextual() && v.equals(lv.data.asText())),"could not find "+v+" in "+lvs);
        });
    }
    @org.junit.jupiter.api.Test
    public void createLabelValues_doubleIter() throws JsonProcessingException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        //start.txn
        tm.begin();
        TestDao t = new TestDao("doubleIter-test");
        LabelDAO foo = new LabelDAO("foo",t)
                .loadExtractors(ExtractorDao.fromString("$.foo").setName("foo"));
        LabelDAO iterFoo = new LabelDAO("iterFoo",t)
                .loadExtractors(ExtractorDao.fromString("foo[]").setName("iterFoo"));
        LabelDAO bar = new LabelDAO("bar",t)
                .loadExtractors(ExtractorDao.fromString("iterFoo:$.bar").setName("bar"));
        LabelDAO iterBar = new LabelDAO("iterBar",t)
                .loadExtractors(ExtractorDao.fromString("bar[]").setName("iterBar"));
        LabelDAO iterBarSum = new LabelDAO("iterBarSum",t)
                .loadExtractors(
                        ExtractorDao.fromString("iterBar:$.key").setName("key"),
                        ExtractorDao.fromString("iterBar:$.value").setName("value")
                );
        iterBarSum.setReducer("({key,value})=>(key||'')+(value||'')");
        t.loadLabels(foo,iterFoo,bar,iterBar,iterBarSum);
        t.persistAndFlush();
        RunDao r = new RunDao(
                t.id,
                new ObjectMapper().readTree("""
                {
                  "foo" : [
                    { "bar": [ {"key":"primero"},{"key":"segundo","value":"two"}] }
                  ]
                }
                """),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        //end.txn
        //do stuff outside
        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValueDao> lvs = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,iterBarSum.id).list();

        assertEquals(2,lvs.size());
        assertEquals("primero",lvs.get(0).data.asText());
        assertEquals("segundotwo",lvs.get(1).data.asText());
    }
    @org.junit.jupiter.api.Test
    public void labelValues_schema_post_iterated() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        TestDao t = new TestDao("target_schema");
        LabelDAO foo = new LabelDAO("foo",t)
                .loadExtractors(ExtractorDao.fromString("$.foo").setName("foo"));
        LabelDAO iterFoo = new LabelDAO("iterFoo",t)
                .loadExtractors(ExtractorDao.fromString("foo[]").setName("iterFoo"))
                .setTargetSchema("direct");
        LabelDAO biz = new LabelDAO("biz",t)
                .loadExtractors(ExtractorDao.fromString("iterFoo:$.biz").setName("biz"));
        LabelDAO buz = new LabelDAO("buz",t)
                .loadExtractors(ExtractorDao.fromString("iterFoo:$.buz").setName("buz"));
        t.loadLabels(foo,iterFoo,biz,buz);
        t.persistAndFlush();
        RunDao r = new RunDao(
                t.id,
                new ObjectMapper().readTree("""
            {
                "foo": [{"biz":"one","buz":"uno"},{"biz":"two","buz":"dos"}]
            }"""),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelService.ValueMap> valueMaps = labelService.labelValues("direct",t.id,Collections.emptyList(),Collections.emptyList());
        assertEquals(2,valueMaps.size());
        assertEquals(new ObjectMapper().readTree(
                """
                {"biz":"one","buz":"uno"}
                """),valueMaps.get(0).data());
        assertEquals(new ObjectMapper().readTree(
                """
                {"biz":"two","buz":"dos"}
                """),valueMaps.get(1).data());
    }
    @org.junit.jupiter.api.Test
    public void labelValues_schema_direct_two_labels() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        TestDao t = new TestDao("target_schema");
        LabelDAO foo = new LabelDAO("foo",t)
                .loadExtractors(ExtractorDao.fromString("$.foo").setName("foo"))
                .setTargetSchema("direct");
        LabelDAO bar = new LabelDAO("bar",t)
                .loadExtractors(ExtractorDao.fromString("$.bar").setName("bar"))
                .setTargetSchema("direct");
        LabelDAO fooBiz = new LabelDAO("fooBiz",t)
                .loadExtractors(
                        ExtractorDao.fromString("foo:$.biz").setName("biz"),
                        ExtractorDao.fromString("foo:$.buz").setName("buz")
                );
        LabelDAO barBiz = new LabelDAO("barBiz",t)
                .loadExtractors(
                        ExtractorDao.fromString("bar:$.biz").setName("biz"),
                        ExtractorDao.fromString("bar:$.buz").setName("buz")
                );
        t.loadLabels(foo,fooBiz,bar,barBiz);
        t.persistAndFlush();
        RunDao r = new RunDao(
                t.id,
                new ObjectMapper().readTree("""
            {
                "foo": {"biz":"one","buz":"uno"},
                "bar": {"biz":"two","buz":"dos"}
            }"""),
                new ObjectMapper().readTree("{}")
        );
        r.persist();
        tm.commit();
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelService.ValueMap> valueMaps = labelService.labelValues("direct",t.id,Collections.emptyList(),Collections.emptyList());
        assertEquals(2,valueMaps.size());
        assertEquals(new ObjectMapper().readTree(
                """
                {"fooBiz": {"biz":"one","buz":"uno"} }
                """),valueMaps.get(0).data());
        assertEquals(new ObjectMapper().readTree(
                """
                {"barBiz": {"biz":"two","buz":"dos"} }
                """),valueMaps.get(1).data());
    }

    @Transactional
    public TestDao createTest_reducing(){
        TestDao t = new TestDao("reducer-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        a1.reducer= new LabelReducerDao("ary=>ary.map(v=>v*2)");
        LabelDAO iterA = new LabelDAO("iterA",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(ExtractorDao.fromString("a1[]").setName("iterA"));
        LabelDAO b1 = new LabelDAO("b1",t)
                .loadExtractors(ExtractorDao.fromString("$.b1").setName("b1"));
        LabelDAO iterB = new LabelDAO("iterB",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(ExtractorDao.fromString("b1[]").setName("iterB"));
        LabelDAO nxn = new LabelDAO("nxn",t)
                .loadExtractors(
                        ExtractorDao.fromString("iterA").setName("foundA"),
                        ExtractorDao.fromString("iterB").setName("foundB")
                );
        nxn.reducer = new LabelReducerDao("({foundA,foundB})=>foundA*foundB");
        nxn.multiType= Label.MultiIterationType.NxN;
        t.loadLabels(a1,b1,iterA,iterB,nxn); // order should not matter
        t.persist();
        return t;
    }

    @Transactional
    public TestDao createTest(){
        TestDao t = new TestDao("example-test");
        LabelDAO a1 = new LabelDAO("a1",t)
                .loadExtractors(ExtractorDao.fromString("$.a1").setName("a1"));
        LabelDAO b1 = new LabelDAO("b1",t)
                .loadExtractors(ExtractorDao.fromString("$.b1").setName("b1"));
        LabelDAO firstAKey = new LabelDAO("firstAKey",t)
                .loadExtractors(ExtractorDao.fromString("a1:$[0].key").setName("firstAKey"));
        LabelDAO justA = new LabelDAO("justA",t)
                .loadExtractors(ExtractorDao.fromString("a1").setName("justA"));
        LabelDAO iterA = new LabelDAO("iterA",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(ExtractorDao.fromString("a1[]").setName("iterA"));
        LabelDAO iterAKey = new LabelDAO("iterAKey",t)
                .setTargetSchema("uri:different:keyed")
                .loadExtractors(ExtractorDao.fromString("a1[]:$.key").setName("iterAKey"));
        LabelDAO iterB = new LabelDAO("iterB",t)
                .setTargetSchema("uri:keyed")
                .loadExtractors(ExtractorDao.fromString("b1[]").setName("iterB"));
        LabelDAO foundA = new LabelDAO("foundA",t)
                .loadExtractors(ExtractorDao.fromString("iterA:$.key").setName("foundA"));
        LabelDAO foundB = new LabelDAO("foundB",t)
                .loadExtractors(ExtractorDao.fromString("iterB:$.key").setName("foundB"));
        LabelDAO nxn = new LabelDAO("nxn",t)
            .loadExtractors(
                    ExtractorDao.fromString("iterA:$.key").setName("foundA"),
                    ExtractorDao.fromString("iterB:$.key").setName("foundB")
            );
        LabelDAO jenkinsBuild = new LabelDAO("build",t)
            .loadExtractors(ExtractorDao.fromString(
                ExtractorDao.METADATA_PREFIX+"metadata"+ ExtractorDao.METADATA_SUFFIX+
                ExtractorDao.NAME_SEPARATOR+ ExtractorDao.PREFIX+".jenkins.build").setName("build")
            );
        nxn.multiType= Label.MultiIterationType.NxN;

        t.loadLabels(justA,foundA,firstAKey,foundB,a1,b1,iterA,iterAKey,iterB,nxn,jenkinsBuild); // order should not matter
        t.persist();
        return t;
    }
    @Transactional
    public RunDao createRun_reducing(TestDao t) throws JsonProcessingException {
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"a1\":[0, 2, 4],\"b1\":[1, 3, 5]}"),new ObjectMapper().readTree("{}"));
        r.persist();
        return r;
    }


    @Transactional
    public RunDao createRun(TestDao t) throws JsonProcessingException {
        return createRun(t,"");
    }
    @Transactional
    public RunDao createRun(TestDao t,String k) throws JsonProcessingException {
        String v = k.isBlank() ? "" : k+"_";
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_"+v+"alpha\"}, {\"key\":\"a1_"+v+"bravo\"}, {\"key\":\"a1_"+v+"charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"b1\": [{\"key\":\"b1_"+v+"alpha\"}, {\"key\":\"b1_"+v+"bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        r.persist();
        return r;
    }
    @Disabled
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_NxN_jsonpath_on_iterated() throws JsonProcessingException {

        TestDao t = createTest();
        RunDao r = createRun(t,"uno");
        LabelDAO nxn = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","nxn",t.id).firstResult();

        labelService.calculateLabelValues(t.labels,r.id);

        List<LabelValueDao> lvs = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,nxn.id).list();
        assertNotNull(lvs,"label_value should exit");
        //TODO this branch does not yet support NxN
        assertEquals(6,lvs.size(),"expect 6 entries: "+lvs);
    }

    @org.junit.jupiter.api.Test
    public void calculateLabelValues_LabelValueExtractor_jsonpath_on_iterated() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        LabelDAO found = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        List<LabelValueDao> lvs = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).list();
        assertNotNull(lvs,"label_value should exit");
        assertEquals(3,lvs.size(),lvs.toString());
    }




    @Transactional
    TestDao createConflictingExtractorTest(){
        TestDao t = new TestDao("example-test");
        LabelDAO key = new LabelDAO("key",t)
            .loadExtractors(
                ExtractorDao.fromString("$.key1").setName("key"),
                ExtractorDao.fromString("$.key2").setName("key"),
                ExtractorDao.fromString("$.key3").setName("key")
            );
        t.loadLabels(key);
        t.persist();
        return t;
    };
    @Transactional
    RunDao createConflictingExtractorRun(TestDao t,String data) throws JsonProcessingException {
        JsonNode node = (new ObjectMapper()).readTree(data);
        RunDao r = new RunDao(t.id,node, JsonNodeFactory.instance.objectNode());
        r.persist();
        return r;
    }
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_extractor_name_conflict_nulls_ignored() throws JsonProcessingException {
        TestDao t = createConflictingExtractorTest();
        RunDao r1 = createConflictingExtractorRun(t,"{\"key2\":\"two\"}");
        labelService.calculateLabelValues(t.labels,r1.id);
        LabelDAO key = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","key",t.id).singleResult();
        List<LabelValueDao> found = LabelValueDao.find("from LabelValueDao lv where lv.label.id=?1 and lv.run.id=?2",key.id,r1.id).list();
        assertNotNull(found);
        assertEquals(1,found.size());
        LabelValueDao lv = found.get(0);
        assertNotNull(lv.data);
        assertTrue(lv.data.has("key"));
        JsonNode value = lv.data.get("key");
        assertNotNull(value);
        assertInstanceOf(TextNode.class,value);
        assertEquals("two",value.asText());
    }
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_extractor_name_conflict_last_match_wins() throws JsonProcessingException {
        TestDao t = createConflictingExtractorTest();
        RunDao r1 = createConflictingExtractorRun(t,"{\"key1\":\"one\",\"key2\":\"two\",\"key3\":\"three\"}");
        labelService.calculateLabelValues(t.labels,r1.id);
        LabelDAO key = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","key",t.id).singleResult();
        List<LabelValueDao> found = LabelValueDao.find("from LabelValueDao lv where lv.label.id=?1 and lv.run.id=?2",key.id,r1.id).list();
        assertNotNull(found);
        assertEquals(1,found.size());
        LabelValueDao lv = found.get(0);
        assertNotNull(lv.data);
        assertTrue(lv.data.has("key"));
        JsonNode value = lv.data.get("key");
        assertNotNull(value);
        assertInstanceOf(TextNode.class,value);
        assertEquals("three",value.asText());
    }



    @Disabled
    @org.junit.jupiter.api.Test
    public void getDerivedValues_iterA() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r1 = createRun(t,"uno");
        RunDao r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        labelService.calculateLabelValues(t.labels,r2.id);
        LabelDAO iterA = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        LabelDAO nxn = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();
        LabelDAO foundA = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        List<LabelValueDao> labelValues = LabelValueDao.find("from LabelValueDao lv where lv.label.id=?1 and lv.run.id=?2",iterA.id,r1.id).list();
        List<LabelValueDao> found = labelService.getDerivedValues(labelValues.get(0),0);
        assertFalse(found.isEmpty(),"found should not be empty");
        assertEquals(7,found.size());
        assertTrue(found.stream().anyMatch(lv->lv.label.equals(nxn)));
        assertTrue(found.stream().anyMatch(lv->lv.label.equals(foundA)));
    }
    @org.junit.jupiter.api.Test
    public void getBySchema_multiple_results() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r = createRun(t);
        labelService.calculateLabelValues(t.labels,r.id);
        List<LabelValueDao> found = labelService.getBySchema("uri:keyed",t.id);

        assertFalse(found.isEmpty(),"found should not be empty");
        assertEquals(5,found.size(),"found should have 2 entries: "+found);
    }

    @Transactional
    @org.junit.jupiter.api.Test
    public void calculateLabelValues_RunMetadataExtractor() throws JsonProcessingException {
        TestDao t = new TestDao("example-test");

        LabelDAO found = new LabelDAO("found",t)
                .loadExtractors(ExtractorDao.fromString("{metadata}:$.jenkins.build").setName("found"));
        t.loadLabels(found);
        t.persist();
        JsonNode a1Node = new ObjectMapper().readTree("[ {\"key\":\"a1_alpha\"}, {\"key\":\"a1_bravo\"}, {\"key\":\"a1_charlie\"}]");
        RunDao r = new RunDao(t.id,
                new ObjectMapper().readTree("{ \"foo\" : { \"bar\" : \"bizz\" }, \"a1\": "+a1Node.toString()+", \"a2\": [{\"key\":\"a2_alpha\"}, {\"key\":\"a2_bravo\"}] }"),
                new ObjectMapper().readTree("{ \"jenkins\" : { \"build\" : \"123\" } }"));
        RunDao.persist(r);

        labelService.calculateLabelValues(t.labels,r.id);

        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r.id,found.id).firstResult();

        assertNotNull(lv,"label_value should exit");
        assertEquals("123",lv.data.asText());
    }

    @org.junit.jupiter.api.Test
    public void labelValues_schema() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r1 = createRun(t,"uno");
        //RunDao r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        //labelService.calculateLabelValues(t.labels,r2.id);
        List<LabelService.ValueMap> labelValues = labelService.labelValues("uri:keyed",t.id, Collections.emptyList(),Collections.emptyList());

        long aCount = labelValues.stream().filter(map->map.data().has("foundA")).count();
        long bCount = labelValues.stream().filter(map->map.data().has("foundB")).count();

        assertEquals(3,aCount);
        assertEquals(2,bCount);
        assertEquals(5,labelValues.size());
    }
    @org.junit.jupiter.api.Test
    public void labelValues_testId() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r1 = createRun(t,"uno");
        labelService.calculateLabelValues(t.labels,r1.id);
        List<LabelService.ValueMap> labelValues = labelService.labelValues(t.id,null,null,null,null,null,Integer.MAX_VALUE,0,Collections.emptyList(),Collections.emptyList(),false);
        assertEquals(1,labelValues.size(),"only one run should exist for the test");
        LabelService.ValueMap map = labelValues.get(0);
        assertEquals(t.id,map.testId(),"test Id should match");
        assertEquals(r1.id,map.runId(),"run Id should match");
        ObjectNode data = map.data();
        assertNotNull(data,"data should not be null");
        assertTrue(data.has("nxn"),"data should have the nxn key");
        assertTrue(data.has("iterA"),"data should have the iterA key");
        assertTrue(data.has("iterB"),"data should have the iterB key");
        assertTrue(data.has("iterAKey"),"data should have the iterAKey key");
        assertTrue(data.has("foundA"),"data should have hte foundA key");
        assertTrue(data.has("foundB"),"data should have hte foundB key");
    }

    @Disabled
    @org.junit.jupiter.api.Test
    public void multiType_NxN() throws JsonProcessingException {
        TestDao t = createTest();
        RunDao r1 = createRun(t,"uno");
        //RunDao r2 = createRun(t,"dos");
        labelService.calculateLabelValues(t.labels,r1.id);
        LabelDAO l = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();
        LabelValueDao lv = LabelValueDao.find("from LabelValueDao lv where lv.run.id=?1 and lv.label.id=?2",r1.id,l.id).firstResult();

        //expect 6 entries given 3 from iterA and 2 from iterB
        assertEquals(6,lv.data.size());
        //make sure the data contains the proper 3 x 2 combinations
        JsonNode expected = (new ObjectMapper()).readTree("[{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_alpha\"},{\"foundA\":\"a1_uno_alpha\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_bravo\",\"foundB\":\"b1_uno_bravo\"},{\"foundA\":\"a1_uno_charlie\",\"foundB\":\"b1_uno_bravo\"}]");
        assertEquals(expected,lv.data,"data did not match expected "+lv.data);
    }

    @org.junit.jupiter.api.Test
    public void getDescendantLabels() throws JsonProcessingException {
        TestDao t = createTest();

        LabelDAO a1 = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","a1",t.id).singleResult();
        LabelDAO firstAKey = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","firstAKey",t.id).singleResult();
        LabelDAO justA = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","justA",t.id).singleResult();
        LabelDAO iterA = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","iterA",t.id).singleResult();
        LabelDAO iterAKey = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","iterAKey",t.id).singleResult();
        LabelDAO foundA = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","foundA",t.id).singleResult();
        LabelDAO nxn = LabelDAO.find("from LabelDAO l where l.name=?1 and l.parent.id=?2","nxn",t.id).singleResult();

        List<LabelDAO> list = labelService.getDescendantLabels(a1.id);

        assertNotNull(list);
        assertFalse(list.isEmpty());
        List<LabelDAO> expected = Arrays.asList(a1,firstAKey,justA,iterA,iterAKey,foundA,nxn);
        assertEquals(expected.size(),list.size(),list.size() > expected.size() ? "extra "+list.stream().filter(v->!expected.contains(v)).toList() : "missing "+expected.stream().filter(v->!list.contains(v)).toList());
        expected.forEach(l->{
            assertTrue(list.contains(l),"descendant should include "+l.name+" : "+list);
        });

    }

}
