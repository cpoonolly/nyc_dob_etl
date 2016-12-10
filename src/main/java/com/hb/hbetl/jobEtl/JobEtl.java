package com.hb.hbetl.jobEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.s3.AmazonS3;
import com.hb.hbetl.statReportEtl.StatFileType;
import org.apache.logging.log4j.LogManager;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hb.hbetl.statReportEtl.StatFileSchema.FILE_REF_COLUMN_NAME;

public class JobEtl implements Runnable {
    public static final String REDSHIFT_JOB_STATUS_TABLE = "job_status";
    public static final String S3_JOB_PREFIX = "nyc.jobs";

    public static final String JOB_PAGE_TEMP_FILE_PREFIX = "hbetl_nyc_dob_job_";
    public static final String JOB_PAGE_TEMP_FILE_DELIMITER = "__";

    private static final org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger(JobEtl.class);

    public JobPageLoader jobPageLoader;
    public JobPageScraper jobPageScraper;

    private final Connection redshiftConnection;
    private final AWSLambda lambda;

    private Exception failure;
    private List<Integer> loadedJobNumbers;
    private List<Integer> processedJobNumbers;

    public JobEtl(Connection redshiftConnection, AWSLambda lambda) {
        this.redshiftConnection = redshiftConnection;
        this.lambda = lambda;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("");
            LOGGER.info("/********************************************************/");
            LOGGER.info("/***                    Job ETL                       ***/");
            LOGGER.info("/********************************************************/");

            // Setup redshift tables
            LOGGER.info("");
            LOGGER.info("Setting up Redshift Tables");
            createJobStatusTable();
            createJobDataTable();
            updateJobStatusTable();

            // Find Jobs to Load
            LOGGER.info("");
            LOGGER.info("Finding jobs to load");
            List<Integer> jobNumbersToLoad = getJobNumbersToLoad();
            jobNumbersToLoad = jobNumbersToLoad.subList(0, 10);
            LOGGER.info("Found " + jobNumbersToLoad.size() + " jobs to load");

            // Load Job Pages
            LOGGER.info("");
            LOGGER.info("Loading job pages");
            jobPageLoader = new JobPageLoader(jobNumbersToLoad, lambda);
            loadedJobNumbers = jobPageLoader.call();
            LOGGER.info("Loaded " + loadedJobNumbers.size() + " job pages");

            LOGGER.info("Failed to load " + jobPageLoader.getFailures().size() + " job pages");
            jobPageLoader.getFailures()
                    .forEach((jobNum, failure) -> LOGGER.debug("Failed to load job: " + jobNum, failure));

            // Process each Job Page
            LOGGER.info("");
            LOGGER.info("Finding jobs to process");
            List<Integer> jobNumbersToProcess = getJobNumbersToProcess();
            LOGGER.info("Found " + jobNumbersToProcess.size() + " jobs to process");

            LOGGER.info("Processing jobs");
            jobPageScraper = new JobPageScraper(jobNumbersToProcess, lambda);
            processedJobNumbers = jobPageScraper.call();
            LOGGER.info("Processed " + processedJobNumbers.size() + " jobs");

            LOGGER.info("Failed to process " + jobPageScraper.getFailures().size() + " jobs");
            jobPageScraper.getFailures()
                    .forEach((jobNum, failure) -> LOGGER.debug("Failed to process job: " + jobNum, failure));

            // Update redshift with failures
            LOGGER.info("");
            LOGGER.info("Uploading failures to Redshift");
            updateJobStatusesWithErrors(jobPageLoader.getFailures());
            updateJobStatusesWithErrors(jobPageScraper.getFailures());
        } catch (Exception e) {
            this.failure = e;
        }
    }

    private void createJobDataTable() throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = JobPageParser.getCreateTableSql();

            statement.execute(sql);
        }
    }

    private void createJobStatusTable() throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = "create table if not exists " + REDSHIFT_JOB_STATUS_TABLE + " (" +
                    "job_num integer not null," +
                    "src_stat_report varchar(50) not null," +
                    "s3_key varchar(100)," +
                    "last_error varchar(100) default null," +
                    "date_last_downloaded timestamp default null," +
                    "date_last_parsed timestamp default null," +
                    "date_created timestamp not null default getdate()" +
                ")";

            statement.execute(sql);
        }
    }

    private void updateJobStatusTable() throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = "insert into " + REDSHIFT_JOB_STATUS_TABLE + " (" +
                    "select distinct to_number(job_num, 9999999999)::integer as job_num, " + FILE_REF_COLUMN_NAME + " as src_stat_report " +
                    "from " + StatFileType.JOB.sqlTableName + " srj " +
                    "where not exists (" +
                        "select 1 from " + REDSHIFT_JOB_STATUS_TABLE + " j where j.job_num = srj.job_num" +
                    ")" +
                ")";

            statement.execute(sql);
        }
    }

    private List<Integer> getJobNumbersToLoad() throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = "select job_num from " + REDSHIFT_JOB_STATUS_TABLE + " where date_last_downloaded is null";

            ResultSet result = statement.executeQuery(sql);
            List<Integer> jobNumbers = new ArrayList<>(result.getFetchSize());
            while (result.next()) {
                jobNumbers.add(result.getInt("job_num"));
            }

            return jobNumbers;
        }
    }

    private List<Integer> getJobNumbersToProcess() throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = String.format("select job_num from %s ", REDSHIFT_JOB_STATUS_TABLE);
            sql += "where date_last_parsed is null ";
            sql += "and date_last_downloaded is not null ";
            sql += "and s3_key is not null";

            ResultSet result = statement.executeQuery(sql);
            List<Integer> jobNumbers = new ArrayList<>(result.getFetchSize());
            while (result.next()) {
                jobNumbers.add(result.getInt("job_num"));
            }

            return jobNumbers;
        }
    }

    private void updateJobStatusesWithErrors(Map<Integer, Exception> jobFailures) throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String updateTemplate = "update " + REDSHIFT_JOB_STATUS_TABLE + " set last_error = '%s' where job_num = %s";

            List<String> updateStatements = jobFailures.entrySet().stream()
                    .map(entry -> String.format(updateTemplate, entry.getValue().getLocalizedMessage(), entry.getKey()))
                    .collect(Collectors.toList());

            statement.execute("begin");

            for (String updateSql : updateStatements) {
                statement.execute(updateSql);
            }

            statement.execute("commit");
        }
    }

    public static URL getUrlForJobNum(Integer jobNum) throws MalformedURLException {
        return new URL(String.format("http://a810-bisweb.nyc.gov/bisweb/JobsQueryByNumberServlet?passjobnumber=%d", jobNum));
    }

    public static String getFilenameForJobNum(Integer jobNum) {
        return String.format("job%s.html", jobNum);
    }

    public static String getS3KeyForForJobNum(Integer jobNum) {
        return String.format("%s/%s", S3_JOB_PREFIX, getFilenameForJobNum(jobNum));
    }

    public boolean isSuccessful() {
        return this.failure == null;
    }

    public Exception getFailure() {
        return this.failure;
    }
}
