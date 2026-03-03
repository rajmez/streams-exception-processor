package com.example.exceptionprocessor.repo;

import com.example.exceptionprocessor.persistence.ExceptionRecord;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ExceptionRecordRepository extends JpaRepository<ExceptionRecord, Long> {
    List<ExceptionRecord> findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(
            Collection<String> securityIds
    );
}
