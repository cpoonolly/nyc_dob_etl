package com.hb.hbetl.jobEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.hb.hbetl.jobEtl.lambdas.LoadJobPageLambda;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Loads the jobPages into S3. Returns the list of successfully processed jobNumbers
 */
public class JobPageLoader implements Callable<List<Integer>> {
    private static final Logger LOGGER = LogManager.getLogger(JobPageLoader.class);

    private final List<Integer> jobNumbers;
    private final AWSLambda lambda;
    private final Proxy proxy;
    private final AtomicInteger progressCounter;

    private Map<Integer, Exception> failedJobs;

    public JobPageLoader(List<Integer> jobNumbers, AWSLambda lambda) {
        this(jobNumbers, lambda, null);
    }

    public JobPageLoader(List<Integer> jobNumbers, AWSLambda lambda, String proxyUrl) {
        this.jobNumbers = jobNumbers;
        this.lambda = lambda;
        this.proxy = (proxyUrl == null) ? null : new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl, 80));
        this.progressCounter = new AtomicInteger();
    }

    @Override
    public List<Integer> call() throws Exception {
        failedJobs = new ConcurrentHashMap<>();

        return jobNumbers.parallelStream()
                .map(this::loadJobPage)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public Map<Integer, Exception> getFailures() {
        return failedJobs;
    }

    private Integer loadJobPage(Integer jobNumber) {
        Integer result = null;
        try {
            result = LambdaInvokerFactory.builder()
                .lambdaClient(lambda)
                .build(LoadJobPageLambda.class)
                .loadJobPage(jobNumber);
        } catch (Exception e) {
            failedJobs.put(jobNumber, e);
        }

        LOGGER.debug("Progress: {}/{}\r", progressCounter.incrementAndGet(), jobNumbers.size());

        return result;
    }
}
