package com.hb.hbetl.statReportEtl.exceptions;

public class StatReportCellParseException extends Exception {
    private final int columnIndex;

    public StatReportCellParseException(int columnIndex, String message) {
        super(message);

        this.columnIndex = columnIndex;
    }
}
