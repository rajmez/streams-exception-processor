package com.example.exceptionservice.repo;

import com.example.exceptionservice.domain.ExceptionRecord;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ExceptionRecordRepository extends JpaRepository<ExceptionRecord, Long> {
    Page<ExceptionRecord> findBySecurityIdAndProcessedAtIsNullOrderByOccurredAtAsc(
            String securityId,
            Pageable pageable
    );
}
