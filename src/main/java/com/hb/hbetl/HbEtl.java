package com.hb.hbetl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.hb.hbetl.jobEtl.JobEtl;
import com.hb.hbetl.statReportEtl.StatReportEtl;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class HbEtl implements Runnable {
    public static final String S3_BUCKET = "cherry.hbetl";
    public static final String REDSHIFT_CREDENTIALS = "aws_iam_role=arn:aws:iam::061254541186:role/hbetl";

    private final AmazonS3 s3;
    private final AWSLambda lambda;
    private final Connection redshiftConnection;

    private static final Logger LOGGER = LogManager.getLogger(HbEtl.class);

    public HbEtl() throws Exception {
        this.redshiftConnection = getRedshiftConnection();

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setSocketTimeout(30 * 60 * 1000);

        this.s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withClientConfiguration(clientConfig)
                .build();

        this.lambda = AWSLambdaClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withClientConfiguration(clientConfig)
            .build();
    }

    public static void main(String[] args) throws Exception {
        new HbEtl().run();
    }

    public static String getFilenameForS3Key(String s3Key) {
        return FilenameUtils.getName(s3Key);
    }

    public static String getFilenameForUrl(URL fileUrl) {
        return FilenameUtils.getName(fileUrl.getPath());
    }

    @Override
    public void run() {
        StatReportEtl statReportEtl = new StatReportEtl(redshiftConnection, s3, lambda)
                .setYearsToLoad(Arrays.asList(2017, 2016));
        statReportEtl.run();

        if (!statReportEtl.isSuccessful()) {
            LOGGER.error("Stat Report ETL failed:", statReportEtl.getFailure());
        }

        JobEtl jobEtl = new JobEtl(redshiftConnection, lambda);
        jobEtl.run();

        if (!jobEtl.isSuccessful()) {
            LOGGER.error("Job ETL failed:", jobEtl.getFailure());
        }
    }

    public static Connection getRedshiftConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.amazon.redshift.jdbc42.Driver");
        String userName = System.getenv("CFG_REDSHIFT_USER_NAME");
        String password = System.getenv("CFG_REDSHIFT_USER_PASS");
        String host = System.getenv("CFG_REDSHIFT_HOST");

        Properties props = new Properties();
        props.setProperty("user", userName);
        props.setProperty("password", password);

        return DriverManager.getConnection(host, props);
    }
}
