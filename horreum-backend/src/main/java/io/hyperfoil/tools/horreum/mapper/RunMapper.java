package io.hyperfoil.tools.horreum.mapper;

import java.util.stream.Collectors;

import io.hyperfoil.tools.horreum.api.data.Run;
import io.hyperfoil.tools.horreum.entity.data.RunDAO;

public class RunMapper {

    public static Run from(RunDAO run) {
        Run dto = new Run();
        dto.id = run.id;
        dto.start = run.start;
        dto.stop = run.stop;
        dto.description = run.description;
        dto.testid = run.testid;
        dto.data = run.data;
        dto.metadata = run.metadata;
        dto.trashed = run.trashed;
        if (run.validationErrors != null)
            dto.validationErrors = run.validationErrors.stream().map(ValidationErrorMapper::fromValidationError)
                    .collect(Collectors.toList());
        if (run.datasets != null)
            dto.datasets = run.datasets.stream().map(DatasetMapper::from).collect(Collectors.toList());
        dto.owner = run.owner;
        dto.access = run.access;

        return dto;
    }

    public static RunDAO to(Run dto) {
        RunDAO run = new RunDAO();
        run.id = dto.id;
        run.start = dto.start;
        run.stop = dto.stop;
        run.description = dto.description;
        run.testid = dto.testid;
        run.data = dto.data;
        run.metadata = dto.metadata;
        run.trashed = dto.trashed;
        if (dto.validationErrors != null) {
            run.validationErrors = dto.validationErrors.stream().map(ValidationErrorMapper::toValidationError)
                    .collect(Collectors.toList());
        }
        if (dto.datasets != null) {
            run.datasets = dto.datasets.stream().map(dsDTO -> DatasetMapper.to(dsDTO, run)).collect(Collectors.toList());
        }

        return run;
    }

}
