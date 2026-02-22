package com.example.exceptionservice.dto;

import com.example.exceptionservice.domain.Severity;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

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
