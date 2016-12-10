package com.hb.hbetl.jobEtl.lambdas;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.jobEtl.JobEtl;
import com.hb.hbetl.jobEtl.JobPageParser;
import com.hb.hbetl.jobEtl.parsers.JobFieldParsingException;
import com.hb.hbetl.jobEtl.validators.JobPageValidationException;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.hb.hbetl.jobEtl.JobEtl.*;

/**
 * Scrapes the given job page and inserts data into redshift
 */
public class ScrapeJobPagesLambdaImpl implements ScrapeJobPagesLambda {
    private final Connection redshiftConnection;
    private final AmazonS3 s3;

    public ScrapeJobPagesLambdaImpl() throws SQLException, ClassNotFoundException {
        redshiftConnection = HbEtl.getRedshiftConnection();
        s3 = AmazonS3ClientBuilder.defaultClient();
    }

    @Override
    public Integer scrapeJobPage(Integer jobNumber) {
        JobPageParser parser = generateJobPageParser(jobNumber);
        if (parser == null) {
            return null;
        }

        try (Statement statement = redshiftConnection.createStatement()) {
            String insertSql = JobPageParser.generateInsertSql(parser);
            String updateSql = generateUpdateSql(parser);

            statement.execute("begin");
            statement.execute(insertSql);
            statement.execute(updateSql);
            statement.execute("commit");

            return jobNumber;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private JobPageParser generateJobPageParser(Integer jobNumber) {
        try {
            String s3Key = JobEtl.getS3KeyForForJobNum(jobNumber);
            S3Object s3Object = s3.getObject(HbEtl.S3_BUCKET, s3Key);

            File jobPage = File.createTempFile(JOB_PAGE_TEMP_FILE_PREFIX, JOB_PAGE_TEMP_FILE_DELIMITER + JobEtl.getFilenameForJobNum(jobNumber));
            FileUtils.copyInputStreamToFile(s3Object.getObjectContent(), jobPage);

            Document document = Jsoup.parse(jobPage, null);

            return new JobPageParser(jobNumber, document);
        } catch (AmazonClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateUpdateSql(JobPageParser parser) {
        return String.format("update %s set date_last_parsed = getdate() where job_num = %d",
                REDSHIFT_JOB_STATUS_TABLE,
                parser.jobNumber);
    }
}
