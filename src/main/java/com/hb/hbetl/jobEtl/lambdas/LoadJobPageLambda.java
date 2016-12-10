package com.hb.hbetl.jobEtl.lambdas;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

/**
 * AWS lambda function for loading html page for the given job number into S3
 */
public interface LoadJobPageLambda {
    @LambdaFunction(functionName = "nyc_dob_load_job_page")
    Integer loadJobPage(Integer jobNumber);
}
