package io.hyperfoil.tools.horreum.entity.user;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.id.enhanced.SequenceStyleGenerator;

import java.util.Set;

@Entity(name = "team")
public class Team extends PanacheEntityBase {

    @Id
    @GenericGenerator(
            name = "teamIdGenerator",
            strategy = "io.hyperfoil.tools.horreum.entity.SeqIdGenerator",
            parameters = {
                    @Parameter(name = SequenceStyleGenerator.SEQUENCE_PARAM, value = "team_id_seq"),
                    @Parameter(name = SequenceStyleGenerator.INCREMENT_PARAM, value = "1"),
            }
    )
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "teamIdGenerator")
    @Column(name="id")
    public Integer id;

    @Column(name = "team_name")
    public String teamName;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "team")
    public Set<TeamMembership> teams;

    public Team(){}

    public Team(String teamName) {
        this.teamName = teamName;
    }
}
