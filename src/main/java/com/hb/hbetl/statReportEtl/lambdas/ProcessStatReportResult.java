package com.hb.hbetl.statReportEtl.lambdas;

import com.hb.hbetl.statReportEtl.exceptions.StatReportRowParseException;

import java.util.List;

public class ProcessStatReportResult {
    public String processedStatReportS3Key;
    public Integer parsedRowCount;
    public Integer failedRowCount;

    public ProcessStatReportResult() {}

    public ProcessStatReportResult(String processedStatReportS3Key, Integer parsedRowCount, List<StatReportRowParseException> failedRowCount) {
        this.processedStatReportS3Key = processedStatReportS3Key;
        this.parsedRowCount = parsedRowCount;
        this.failedRowCount = failedRowCount.size();
    }

    public Double failedPercentage() {
        return Double.valueOf(failedRowCount) / (double) (parsedRowCount + failedRowCount);
    }
}
