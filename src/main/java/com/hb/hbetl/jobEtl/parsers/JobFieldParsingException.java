package com.hb.hbetl.jobEtl.parsers;

public class JobFieldParsingException extends RuntimeException {
    private static final long serialVersionUID = -5077038908875561104L;

    public JobFieldParsingException(String jobField, String message) {
        super(String.format("Failed to parse field: %s\nReason: %s", jobField, message));
    }
}
