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
            @Index(name = "idx_exception_security_id", columnList = "securityId")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExceptionRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String serviceName;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Severity severity;

    @Column(nullable = false)
    private String message;

    @Column(nullable = false)
    private Instant occurredAt;

    @Column(nullable = false)
    private String securityId;

    /**
     * Marks that this exception record has been successfully re-driven downstream.
     * Used for idempotency: we only publish records where {@code processedAt} is null.
     */
    @Column
    private Instant processedAt;

    private String stacktrace;

    private String correlationId;
}
