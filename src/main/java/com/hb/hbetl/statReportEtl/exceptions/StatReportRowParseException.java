package com.hb.hbetl.statReportEtl.exceptions;

public class StatReportRowParseException extends Exception {
    public final String statReportFilename;
    public final int rowNum;

    public StatReportRowParseException(String statReportFilename, int rowNum, String message) {
        super(message);

        this.statReportFilename = statReportFilename;
        this.rowNum = rowNum;
    }

    public StatReportRowParseException(String statReportFilename, int rowNum, StatReportCellParseException cellException) {
        super(cellException);

        this.statReportFilename = statReportFilename;
        this.rowNum = rowNum;
    }

    @Override
    public String getMessage() {
        return "file: " + statReportFilename + ", row: " + rowNum + ", error: " + super.getMessage();
    }
}
