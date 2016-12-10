package com.hb.hbetl.jobEtl.validators;

public class JobPageValidationException extends RuntimeException {
    private static final long serialVersionUID = -1533519287692739644L;

    public JobPageValidationException(String message) {
        super(message);
    }
}
