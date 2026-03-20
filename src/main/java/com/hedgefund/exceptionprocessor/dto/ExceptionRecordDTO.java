package com.hedgefund.exceptionprocessor.dto;

import com.hedgefund.exceptionprocessor.persistence.Severity;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Data Transfer Object (DTO) sent to Kafka.
 *
 *DTO means a lightweight object for moving data across boundaries (service -> Kafka),
 * separate from the JPA entity that maps to the database table.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExceptionRecordDTO {
    private Long id;
    private String serviceName;
    private Severity severity;
    private String message;
    private Instant occurredAt;
    private String correlationId;
    private String securityId;
}
