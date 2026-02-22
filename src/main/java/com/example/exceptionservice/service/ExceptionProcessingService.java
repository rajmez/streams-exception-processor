package com.example.exceptionservice.service;

import com.example.exceptionservice.config.AppProperties;
import com.example.exceptionservice.domain.ExceptionRecord;
import com.example.exceptionservice.dto.ExceptionRecordDTO;
import com.example.exceptionservice.repo.ExceptionRecordRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExceptionProcessingService {
    private final ExceptionRecordRepository repo;
    private final KafkaPublisher publisher;
    private final AppProperties props;

    /**
     * Prevents duplicate concurrent processing for the same securityId within a single instance.
     * Redis consumer groups may still deliver duplicate or repeated messages; this avoids wasting
     * local worker capacity.
     */
    private final Set<String> inFlightSecurityIds = ConcurrentHashMap.newKeySet();

    @Async("processingExecutor")
    public CompletableFuture<Void> publishAllBySecurityIdAsync(String securityId) {
        if (!inFlightSecurityIds.add(securityId)) {
            log.debug("securityId {} already in-flight; skipping duplicate trigger", securityId);
            return CompletableFuture.completedFuture(null);
        }

        try {
            publishAllBySecurityIdInternal(securityId);
            return CompletableFuture.completedFuture(null);
        } finally {
            inFlightSecurityIds.remove(securityId);
        }
    }

    @Transactional
    private void publishAllBySecurityIdInternal(String securityId) {
        int pageSize = Math.max(100, props.getPaging().getPageSize());
        int page = 0;
        long totalSent = 0;

        while (true) {
            Page<ExceptionRecord> batch = repo.findBySecurityIdAndProcessedAtIsNullOrderByOccurredAtAsc(
                    securityId,
                    PageRequest.of(page, pageSize)
            );

            if (batch.isEmpty()) {
                break;
            }

            List<CompletableFuture<Void>> publishFutures = new ArrayList<>(batch.getNumberOfElements());
            List<ExceptionRecord> records = batch.getContent();

            for (ExceptionRecord rec : records) {
                ExceptionRecordDTO dto = ExceptionRecordDTO.builder()
                        .id(rec.getId())
                        .serviceName(rec.getServiceName())
                        .severity(rec.getSeverity())
                        .message(rec.getMessage())
                        .occurredAt(rec.getOccurredAt())
                        .correlationId(rec.getCorrelationId())
                        .securityId(rec.getSecurityId())
                        .build();

                publishFutures.add(publisher.publishAsync(props.getKafka().getTopic(), dto));
            }

            CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();

            Instant now = Instant.now();
            for (ExceptionRecord rec : records) {
                rec.setProcessedAt(now);
            }
            repo.saveAll(records);
            totalSent += records.size();

            if (!batch.hasNext()) {
                break;
            }

            page++;
        }

        log.info("Published {} records for securityId={}", totalSent, securityId);
    }
}
