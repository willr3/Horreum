package io.hyperfoil.tools.horreum.entity.data;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.horreum.entity.SeqIdGenerator;
import io.hyperfoil.tools.horreum.hibernate.JsonBinaryType;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.annotations.Type;

import static jakarta.persistence.GenerationType.SEQUENCE;
import static org.hibernate.id.OptimizableGenerator.INCREMENT_PARAM;
import static org.hibernate.id.enhanced.SequenceStyleGenerator.SEQUENCE_PARAM;

@Entity(name = "Action")
public class ActionDAO extends PanacheEntityBase {
   @Id
   @GenericGenerator(
         name = "actionSequence",
         type = SeqIdGenerator.class,
         parameters = { @Parameter(name = SEQUENCE_PARAM, value = "action_id_seq"), @Parameter(name = INCREMENT_PARAM, value = "1") }
   )
   @GeneratedValue(strategy = SEQUENCE, generator = "actionSequence")
   public Integer id;

   @NotNull
   @Column(name = "event")
   public String event;

   /* The type options were found in horreum-web/src/domain/actions/ActionComponentForm.tsx#L73
    * http, github-issue-comment, github-issue-create
    *
    */
   @NotNull
   @Column(name = "type")
   public String type;


   /*
    * Notes on where I found the config options
    * type=http: HttpActionUrlSelector.tsx : {url: string }
    * type=github-issue-comment: { issueUrl: string|undefined, owner: string, repo: string, issue: string, formatter: string }
    * type=github-issue-create: {owner: string, repo: string, title: string, formatter: string }
    */
   @NotNull
   @Column(name = "config", columnDefinition = "jsonb")
   @Type(JsonBinaryType.class)
   public ObjectNode config;

   /*
    * horreum-web/src/domain/actions/ActionComponentForm.tsx#L278
    * { token: string, modified: boolean } are the possible values of secret but I don't think modified gets persisted
    */
   @NotNull
   @Column(name = "secrets", columnDefinition = "jsonb")
   @Type(JsonBinaryType.class)
   public ObjectNode secrets;

   @NotNull
   @Column(name = "test_id")
   public Integer testId;

   @NotNull
   @Transient
   public boolean active = true;

   @NotNull
   @Column(name = "run_always")
   public boolean runAlways;

   public ActionDAO() {
   }
    public ActionDAO(Integer id, String event, String type, ObjectNode config, ObjectNode secrets,
                     Integer testId, boolean active, boolean runAlways) {
       this.id = id;
       this.event = event;
       this.type = type;
       this.config = config;
       this.secrets = secrets;
       this.testId = testId;
       this.active = active;
       this.runAlways = runAlways;
    }

}
