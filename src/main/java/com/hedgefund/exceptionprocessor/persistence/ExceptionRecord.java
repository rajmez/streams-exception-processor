package com.hedgefund.exceptionprocessor.persistence;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity mapped to the `exceptions` table in Postgres.
 *
 *`Entity` is Java/JPA terminology for a class whose fields are persisted as table columns.
 * This project reads unprocessed rows from this table and republishes them to Kafka.
 */
@Entity
@Table(
        name = "exceptions",
        indexes = {
            // Index speeds up queries by securityId, which is our primary lookup key.
            @Index(name = "idx_exception_security_id", columnList = "securityId")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExceptionRecord {
    // Primary key in the table.
    @Id
    // DB auto-generates numeric ids (identity/serial behavior).
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Service that emitted the exception.
    @Column(nullable = false)
    private String serviceName;

    // Persist enum as readable text (LOW/MEDIUM/HIGH/CRITICAL), not ordinal number.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Severity severity;

    // Exception/error message text.
    @Column(nullable = false)
    private String message;

    // When the exception occurred.
    @Column(nullable = false)
    private Instant occurredAt;

    // Business identifier used to find records for each incoming Redis event.
    @Column(nullable = false)
    private String securityId;

    /**
     * Marks that this exception record has been successfully re-driven downstream.
     * Used for idempotency: we only publish records where {@code processedAt} is null.
     */
    @Column
    private Instant processedAt;

    // Optional stacktrace payload for deeper diagnostics.
    private String stacktrace;

    // Optional trace id to correlate with request-level logs.
    private String correlationId;
}
