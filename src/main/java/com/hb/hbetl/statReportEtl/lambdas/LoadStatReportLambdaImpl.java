package com.hb.hbetl.statReportEtl.lambdas;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.hb.hbetl.HbEtl.S3_BUCKET;
import static com.hb.hbetl.HbEtl.getFilenameForUrl;
import static com.hb.hbetl.statReportEtl.StatReportEtl.getS3KeyForStatReport;
import static com.hb.hbetl.statReportEtl.StatReportLoader.XLS_TEMP_FILE_DELIMITER;
import static com.hb.hbetl.statReportEtl.StatReportLoader.XLS_TEMP_FILE_PREFIX;

/**
 * Implementation of {@link LoadStatReportLambda}
 */
public class LoadStatReportLambdaImpl implements LoadStatReportLambda {
    private final AmazonS3 s3;

    public LoadStatReportLambdaImpl() {
        this.s3 = AmazonS3ClientBuilder.defaultClient();
    }

    @Override
    public String loadStatReport(URL fileUrl) {
        try {
            String s3Key = getS3KeyForStatReport(fileUrl);

            // create temp file in local file system
            File statReportFile = File.createTempFile(XLS_TEMP_FILE_PREFIX, XLS_TEMP_FILE_DELIMITER + getFilenameForUrl(fileUrl));

            // load the file into temp file
            FileUtils.copyURLToFile(fileUrl, statReportFile);

            // upload a copy of the file to s3
            s3.putObject(S3_BUCKET, s3Key, statReportFile);

            return s3Key;
        } catch (AmazonClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
