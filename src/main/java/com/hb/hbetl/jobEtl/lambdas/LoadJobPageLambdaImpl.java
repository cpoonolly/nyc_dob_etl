package com.hb.hbetl.jobEtl.lambdas;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.jobEtl.JobEtl;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.hb.hbetl.jobEtl.JobEtl.REDSHIFT_JOB_STATUS_TABLE;

/**
 * Implementation of {@link LoadJobPageLambda}
 */
public class LoadJobPageLambdaImpl implements LoadJobPageLambda {
    private final Connection redshiftConnection;
    private final AmazonS3 s3;

    public LoadJobPageLambdaImpl() throws SQLException, ClassNotFoundException {
        redshiftConnection = HbEtl.getRedshiftConnection();
        s3 = AmazonS3ClientBuilder.defaultClient();
    }

    @Override
    public Integer loadJobPage(Integer jobNumber) {
        try {
            String s3Key = JobEtl.getS3KeyForForJobNum(jobNumber);

            loadJobPageToS3(jobNumber, s3Key);
            updateJobStatusInRedshift(jobNumber, s3Key);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }

        return jobNumber;
    }

    private void updateJobStatusInRedshift(Integer jobNumber, String s3Key) throws SQLException {
        try (Statement statement = redshiftConnection.createStatement()) {
            String sql = "update " + REDSHIFT_JOB_STATUS_TABLE + " " +
                    "set date_last_downloaded = getdate(), s3_key = '" + s3Key + "' " +
                    "where job_num = " + jobNumber;

            statement.execute(sql);
        }
    }

    private void loadJobPageToS3(Integer jobNumber, String s3Key) throws IOException {
        URL jobPageUrl = JobEtl.getUrlForJobNum(jobNumber);

        // create temp file in local file system
        File jobPage = File.createTempFile(JobEtl.JOB_PAGE_TEMP_FILE_PREFIX, JobEtl.JOB_PAGE_TEMP_FILE_DELIMITER + JobEtl.getFilenameForJobNum(jobNumber));

        // load the file into temp file
        FileUtils.copyURLToFile(jobPageUrl, jobPage);

        // upload a copy of the file to s3
        s3.putObject(HbEtl.S3_BUCKET, s3Key, jobPage);
    }
}
