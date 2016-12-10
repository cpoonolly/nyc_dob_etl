package com.hb.hbetl.statReportEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.statReportEtl.exceptions.InvalidStatFileException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ETL processs for loading the data from the nyc dob weekly statistical reports into redshift
 * (see: http://www1.nyc.gov/site/buildings/about/job-statistical-reports.page)
 *
 * These reports are used to keep track of incoming jobs and permits from the nyc dob
 */
public class StatReportEtl implements Runnable {
    public static final String S3_STAT_REPORT_PREFIX = "nyc.stat-reports";
    public static final String S3_STAT_REPORT_PROCESSED_PREFIX = "nyc.stat-reports-processed";

    private static final Logger LOGGER = LogManager.getLogger(StatReportEtl.class);

    public StatReportUrlScraper statReportUrlScraper;
    public StatReportLoader statReportLoader;
    public StatReportProcessor statReportProcessor;

    private final Connection redshiftConnection;
    private final AmazonS3 s3;
    private final AWSLambda lambda;

    private List<Integer> yearsToLoad;
    private Exception failure;

    public StatReportEtl(Connection redshiftConnection, AmazonS3 s3, AWSLambda lambda) {
        this.redshiftConnection = redshiftConnection;
        this.s3 = s3;
        this.lambda = lambda;
    }

    public StatReportEtl setYearsToLoad(List<Integer> yearsToLoad) {
        this.yearsToLoad = yearsToLoad;
        return this;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("");
            LOGGER.info("/********************************************************/");
            LOGGER.info("/***            Stat Report ETL                       ***/");
            LOGGER.info("/********************************************************/");

            // Scrape Stat File URLs
            LOGGER.info("");
            LOGGER.info("Scraping Stat Report file links for years: {}",
                yearsToLoad.stream().map(Object::toString).collect(Collectors.joining(", ")));

            statReportUrlScraper = new StatReportUrlScraper(yearsToLoad);
            List<URL> statReportFileUrls = statReportUrlScraper.call();

            LOGGER.info("Scraped " + statReportFileUrls.size() + " file links");
            LOGGER.info("Stat Report Failure count: " + statReportUrlScraper.getFailures().size());
            statReportUrlScraper.getFailures()
                    .forEach(failure -> LOGGER.debug("Stat Report Failure", failure));
            LOGGER.info("Malformed Stat Report Links count: " + statReportUrlScraper.getMalformedLinks().size());
            statReportUrlScraper.getMalformedLinks()
                    .forEach(malformedUrl -> LOGGER.debug("Malformed URL: " + malformedUrl));

            // Download Stat Files from URLs
            LOGGER.info("");
            LOGGER.info("Loading stat reports");

            statReportLoader = new StatReportLoader(statReportFileUrls, s3, lambda).reloadAll(true);
            List<String> statReportS3Keys = statReportLoader.call();

            LOGGER.info("Loaded " + statReportS3Keys.size() + " reports");
            LOGGER.info("Failed to load " + statReportLoader.getFailuresByFilename().size() + " files");
            statReportLoader.getFailuresByFilename().forEach(LOGGER::debug);

            // Get Stat files to reprocess
            LOGGER.info("");
            LOGGER.info("Find stat reports to reprocess");
            List<String> statFilesToReprocess = getKeysToReprocessFromS3(Pattern.compile(".*job\\d+1[67]\\.xls"));
            LOGGER.info("Found " + statFilesToReprocess.size() + " for reprocessing");
            statReportS3Keys.addAll(statFilesToReprocess);

            // Process downloaded stat files
            LOGGER.info("");
            LOGGER.info("Processing stat reports");

            LOGGER.info("Processing " + statReportS3Keys.size() + " files");
            statReportProcessor = new StatReportProcessor(statReportS3Keys, lambda);
            List<String> uploadedS3Files = statReportProcessor.call();

            LOGGER.info("Processed " + uploadedS3Files.size() + " stat reports");
            LOGGER.info("Parsed " + statReportProcessor.getParsedRowCount() + " rows");

            LOGGER.info("Failed to process " + statReportProcessor.getFailuresByFilename().size() + " files");
            statReportProcessor.getFailuresByFilename().forEach(LOGGER::debug);

            List<String> filesWithHighFailures = statReportProcessor.getFilesWithHighFailedRowPercentages(.9);
            LOGGER.info(filesWithHighFailures.size() + " files with >= 90% of their rows unable to be parsed");
            filesWithHighFailures.forEach(LOGGER::debug);

            // Copy parsed stat file info to redshift
            LOGGER.info("");
            LOGGER.info("Copying parsed stat file data to redshift");
            copyStatDataToRedshift(StatFileType.JOB);
        } catch (Exception e) {
            this.failure = e;
        }
    }

    public void copyStatDataToRedshift(StatFileType fileType) throws IOException, SQLException {
        createTableIfNotExists(fileType);
        truncateTable(fileType);
        copyCsvDataToTable(fileType);
    }

    public void createTableIfNotExists(StatFileType fileType) throws SQLException, IOException {
        StatFileSchema schema = StatFileSchema.getSchemaForFileType(fileType);

        try (Statement statement = redshiftConnection.createStatement()) {
            statement.execute(schema.getSqlTableDefinition());
        }
    }

    public void truncateTable(StatFileType fileType) throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            statement.execute(String.format("truncate table %s", fileType.sqlTableName));
        }
    }

    public void copyCsvDataToTable(StatFileType fileType) throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            statement.execute(String.format("copy %s from 's3://%s/%s' credentials '%s'",
                    fileType.sqlTableName,
                    HbEtl.S3_BUCKET,
                    StatReportEtl.S3_STAT_REPORT_PROCESSED_PREFIX,
                    HbEtl.REDSHIFT_CREDENTIALS));
        }
    }

    public boolean isSuccessful() {
        return this.failure == null;
    }

    public Exception getFailure() {
        return this.failure;
    }

    public static String getS3KeyForStatReport(URL statReportUrl) {
        return String.format("%s/%s", S3_STAT_REPORT_PREFIX, HbEtl.getFilenameForUrl(statReportUrl));
    }

    private StatFileType getFileTypeForS3Key(String s3Key) {
        try {
            return StatFileType.getFileTypeFromFilename(HbEtl.getFilenameForS3Key(s3Key));
        } catch (InvalidStatFileException e) {
            throw new IllegalStateException("processed csv has invalid s3 key", e);
        }
    }

    private List<String> getKeysToReprocessFromS3(Pattern reprocessS3KeysWithPattern) {
        if (reprocessS3KeysWithPattern == null)
            return Collections.emptyList();

        return StatReportEtl.getStatReportsInS3(s3)
                .parallelStream()
                .filter(s3Key -> reprocessS3KeysWithPattern.matcher(s3Key).matches())
                .collect(Collectors.toList());
    }

    public static Set<String> getStatReportsInS3(AmazonS3 s3) {
        return s3.listObjects(HbEtl.S3_BUCKET, StatReportEtl.S3_STAT_REPORT_PREFIX)
                .getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toSet());
    }
}
