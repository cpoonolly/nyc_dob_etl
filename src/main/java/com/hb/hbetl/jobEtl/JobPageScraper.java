package com.hb.hbetl.jobEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.hb.hbetl.jobEtl.lambdas.ScrapeJobPagesLambda;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Scrapes job data from s3 into redshift. Returns the list of successfully processed jobNumbers
 */
public class JobPageScraper implements Callable<List<Integer>> {
    private static final Logger LOGGER = LogManager.getLogger(JobPageScraper.class);

    private final List<Integer> jobNumbers;
    private final AWSLambda lambda;
    private final AtomicInteger progressCounter;

    private Map<Integer, Exception> processingFailures;

    public JobPageScraper(List<Integer> jobNumbers, AWSLambda lambda) {
        this.jobNumbers = jobNumbers;
        this.lambda = lambda;
        this.progressCounter = new AtomicInteger();
    }

    @Override
    public List<Integer> call() throws Exception {
        processingFailures = new ConcurrentHashMap<>();

        return jobNumbers.stream()
                .map(this::scrapeJobPage)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Integer scrapeJobPage(Integer jobNumber) {
        Integer result = null;
        try {
            result = LambdaInvokerFactory.builder()
                    .lambdaClient(lambda)
                    .build(ScrapeJobPagesLambda.class)
                    .scrapeJobPage(jobNumber);
        } catch (Exception e) {
            processingFailures.put(jobNumber, e);
        }

        LOGGER.debug("Progress: {}/{}\r", progressCounter.incrementAndGet(), jobNumbers.size());

        return result;
    }

    public Map<Integer, Exception> getFailures() {
        return processingFailures;
    }
}
