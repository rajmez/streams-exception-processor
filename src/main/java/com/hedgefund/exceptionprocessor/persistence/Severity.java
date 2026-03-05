package com.hedgefund.exceptionprocessor.persistence;

// Shared severity vocabulary used in DB records and Kafka DTO payloads.
public enum Severity {
    // Minor issue, usually non-blocking behavior.
    LOW,
    // Noticeable issue, but typically still recoverable.
    MEDIUM,
    // Serious issue requiring prompt attention.
    HIGH,
    // Most severe level; may indicate outage or data risk.
    CRITICAL
}
