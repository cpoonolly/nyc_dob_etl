package com.hb.hbetl.statReportEtl.exceptions;

public class StatFileProcessingException extends RuntimeException {
    final String filename;
    final Exception e;

    public StatFileProcessingException(String filename, Exception e) {
        this.filename = filename;
        this.e = e;
    }
}
