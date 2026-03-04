package com.example.exceptionprocessor.repo;

import com.example.exceptionprocessor.persistence.ExceptionRecord;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for `ExceptionRecord`.
 *
 *Repository in Spring is a data-access abstraction. By extending `JpaRepository`,
 * we get CRUD methods without writing SQL manually.
 */
public interface ExceptionRecordRepository extends JpaRepository<ExceptionRecord, Long> {
    /**
     * Derived query method name interpreted by Spring Data JPA.
     *
     *Meaning:
     * - `securityId in (...)`
     * - `processedAt is null` (not yet published)
     * - sort oldest first by `occurredAt`
     */
    List<ExceptionRecord> findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(
            Collection<String> securityIds
    );
}
