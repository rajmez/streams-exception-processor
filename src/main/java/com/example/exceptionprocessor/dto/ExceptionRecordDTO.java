package com.example.exceptionprocessor.dto;

import com.example.exceptionprocessor.persistence.Severity;
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
    // Database record identifier for traceability downstream.
    private Long id;
    // Name of the source microservice that produced the original exception.
    private String serviceName;
    // Severity enum lets downstream consumers filter/route alerts.
    private Severity severity;
    // Human-readable error message.
    private String message;
    // Original event timestamp from the exception record.
    private Instant occurredAt;
    // Correlation id ties this exception to broader request tracing.
    private String correlationId;
    // Business key used throughout this project for lookup and replay.
    private String securityId;
}
