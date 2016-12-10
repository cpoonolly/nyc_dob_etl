package com.hb.hbetl.statReportEtl.exceptions;

import java.io.IOException;

public class InvalidStatFileException extends IOException {
    public InvalidStatFileException(String filename) {
        super("Invalid file: " + filename);
    }

    public InvalidStatFileException(String filename, Exception e) {
        super("Invalid file: " + filename, e);
    }
}
