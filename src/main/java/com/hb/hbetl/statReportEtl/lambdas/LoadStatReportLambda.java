package com.hb.hbetl.statReportEtl.lambdas;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

import java.net.URL;

/**
 * AWS lambda function for loading a stat report
 */
public interface LoadStatReportLambda {
    @LambdaFunction(functionName = "nyc_dob_load_stat_report")
    String loadStatReport(URL fileUrl);
}
