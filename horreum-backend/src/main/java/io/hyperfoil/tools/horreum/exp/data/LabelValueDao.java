package io.hyperfoil.tools.horreum.exp.data;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.horreum.hibernate.JsonBinaryType;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Entity
@Table(
    name = "exp_label_values",
    uniqueConstraints = {
        @UniqueConstraint(columnNames = {"run_id","label_id"})
    }
    //indexes used by LabelService.calculateExtractedValuesWithIterated
/*    indexes = {
        @Index(name="lv_run_id",columnList = "run_id",unique = false),
        @Index(name="lv_label_id",columnList = "label_id",unique = false)
    }*/
)
public class LabelValueDao extends PanacheEntity {


    @ManyToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER )
    @JoinTable(name = "exp_label_value_sources",uniqueConstraints = {},joinColumns = {
            @JoinColumn(table = "exp_label_values_")
    })
    public Set<LabelValueDao> sources;//what label_values were used to create this label_value

    @ManyToOne
    @JoinColumn(name="run_id")
    public RunDao run;

    @ManyToOne
    @JoinColumn(name="label_id")
    public LabelDAO label;

    @Type(JsonBinaryType.class)
    public JsonNode data;

    public int ordinal;//used to keep extracted values together when coming from an iterated source

    public LabelValueDao() {
        sources = new HashSet<>();
    }

    public void addSource(LabelValueDao source){
        sources.add(source);
    }
    public void removeSource(LabelValueDao source){
        sources.remove(source);
    }


    @Override
    public String toString(){
        return String.format("LabelValue[id=%d, run_id=%d, label_id=%d, ordinal=%d, data=%s]",id,run.id,label.id,ordinal,data==null ? "null":data.toString());
    }


    @Override
    public int hashCode(){
        return Objects.hash(id);
    }
    @Override
    public boolean equals(Object object){
        return object instanceof LabelValueDao &&
                this.run.equals(((LabelValueDao)object).run) &&
                this.label.equals(((LabelValueDao)object).label);
    }
}
