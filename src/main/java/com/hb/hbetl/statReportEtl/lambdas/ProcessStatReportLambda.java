package com.hb.hbetl.statReportEtl.lambdas;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

/**
 * AWS lambda function for processing a stat report
 */
public interface ProcessStatReportLambda {

    /**
     * @param statReportXlsS3Key s3Key for loaded statReport xls
     * @return s3Key for statReport after it has been processed into a redshift copyable csv
     */
    @LambdaFunction(functionName = "nyc_dob_process_stat_report")
    ProcessStatReportResult processS3StatReport(String statReportXlsS3Key);
}
