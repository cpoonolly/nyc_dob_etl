package com.hb.hbetl.statReportEtl.exceptions;

public class InvalidStatSchemaException extends InvalidStatFileException {
    public InvalidStatSchemaException(String filename) {
        super(filename);
    }

    public InvalidStatSchemaException(String filename, Exception e) {
        super(filename, e);
    }
}
