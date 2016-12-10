package com.hb.hbetl.statReportEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.statReportEtl.lambdas.LoadStatReportLambda;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Loads any stat reports not already present in s3
 */
public class StatReportLoader implements Callable<List<String>> {
    public static final String XLS_TEMP_FILE_PREFIX = "hbetl_nyc_dob_stat_";
    public static final String XLS_TEMP_FILE_DELIMITER = "__";

    private static final Logger LOGGER = LogManager.getLogger(StatReportLoader.class);

    private final AWSLambda lambda;
    private final AmazonS3 s3;
    private final List<URL> statReportUrls;
    private final AtomicInteger progressCounter;

    private Set<String> statReportsInS3;
    private Map<String, Exception> failuresByFilename;

    private boolean reloadAll = false;

    public StatReportLoader(List<URL> statReportUrls, AmazonS3 s3, AWSLambda lambda) {
        this.statReportUrls = statReportUrls;
        this.s3 = s3;
        this.lambda = lambda;
        this.progressCounter = new AtomicInteger();
    }

    public Map<String, Exception> getFailuresByFilename() {
        return failuresByFilename;
    }

    public StatReportLoader reloadAll(boolean reloadAllFlag) {
        this.reloadAll = reloadAllFlag;

        return this;
    }

    @Override
    public List<String> call() throws Exception {
        failuresByFilename = new ConcurrentHashMap<>();
        statReportsInS3 = StatReportEtl.getStatReportsInS3(s3);

        return statReportUrls.parallelStream()
                .filter(url -> !statReportsInS3.contains(StatReportEtl.getS3KeyForStatReport(url)) || reloadAll)
                .map(this::loadStatReportFile)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    // TODO - This could potentially instead return an input streams?
    private String loadStatReportFile(URL fileUrl) {
        String result = null;
        try {
            result = LambdaInvokerFactory.builder()
                .lambdaClient(lambda)
                .build(LoadStatReportLambda.class)
                .loadStatReport(fileUrl);
        } catch (Exception e) {
            failuresByFilename.put(HbEtl.getFilenameForUrl(fileUrl), e);
        }

        LOGGER.debug("Progress: {}/{}\r", progressCounter.incrementAndGet(), statReportUrls.size());

        return result;
    }

}
