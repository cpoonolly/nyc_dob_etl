package com.hb.hbetl.jobEtl.lambdas;

import com.amazonaws.services.lambda.invoke.LambdaFunction;

/**
 * AWS lambda function for scraping job data from job page
 */
public interface ScrapeJobPagesLambda {
    @LambdaFunction(functionName = "nyc_dob_scrape_job_page")
    Integer scrapeJobPage(Integer jobNum);
}
