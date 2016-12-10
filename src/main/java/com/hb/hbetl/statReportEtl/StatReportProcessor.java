package com.hb.hbetl.statReportEtl;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.invoke.LambdaInvokerFactory;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.statReportEtl.exceptions.StatReportCellParseException;
import com.hb.hbetl.statReportEtl.lambdas.ProcessStatReportLambda;
import com.hb.hbetl.statReportEtl.lambdas.ProcessStatReportResult;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;

import java.io.File;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Processes the newly loaded stat files generating csv data which can then be copied into redshift
 */
public class StatReportProcessor implements Callable<List<String>> {
    public static final String CSV_VALUE_DELIMITER = "|";
    public static final String CSV_TEMP_FILE_DELIMITER = StatReportLoader.XLS_TEMP_FILE_DELIMITER;
    public static final String CSV_TEMP_FILE_PREFIX = StatReportLoader.XLS_TEMP_FILE_PREFIX;

    private static final Logger LOGGER = LogManager.getLogger(StatReportProcessor.class);

    private final List<String> statReportS3Keys;
    private final AWSLambda lambda;
    private final AtomicInteger progressCounter;

    private Map<String, Exception> failuresByFilename;
    private List<ProcessStatReportResult> results;

    public StatReportProcessor(List<String> statReportS3Keys, AWSLambda lambda) {
        this.statReportS3Keys = statReportS3Keys;
        this.lambda = lambda;
        this.progressCounter = new AtomicInteger();
    }

    public Map<String, Exception> getFailuresByFilename() {
        return failuresByFilename;
    }

    public Integer getParsedRowCount() {
        return results.stream().mapToInt(result -> result.parsedRowCount).sum();
    }

    public List<String> getFilesWithHighFailedRowPercentages(double highFailedRowPercentage) {
        return results.stream()
                .filter(result -> result.failedPercentage() > highFailedRowPercentage)
                .map(result -> result.processedStatReportS3Key)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> call() throws Exception {
        failuresByFilename = new ConcurrentHashMap<>();

        results = statReportS3Keys.stream()
                .map(this::processStatReport)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return results.stream()
                .map(result -> result.processedStatReportS3Key)
                .collect(Collectors.toList());
    }

    public ProcessStatReportResult processStatReport(String statReportS3Key) {
        ProcessStatReportResult result = null;
        try {
            result = LambdaInvokerFactory.builder()
                .lambdaClient(lambda)
                .build(ProcessStatReportLambda.class)
                .processS3StatReport(statReportS3Key);
        } catch (Exception e) {
            failuresByFilename.put(HbEtl.getFilenameForS3Key(statReportS3Key), e);
        }

        LOGGER.debug("Progress: {}/{}\r", progressCounter.incrementAndGet(), statReportS3Keys.size());

        return result;
    }

    public static String getCsvFilenameForStatReport(File statReport) {
        String filename = parseOriginalStatReportFilename(statReport);

        return FilenameUtils.getBaseName(filename) + ".csv";
    }

    public static String getCsvS3KeyForStatReport(File statReport) {
        return String.format("%s/%s", StatReportEtl.S3_STAT_REPORT_PROCESSED_PREFIX, getCsvFilenameForStatReport(statReport));
    }

    public static String parseOriginalStatReportFilename(File statReport) {
        String filename = FilenameUtils.getName(statReport.getPath());

        return filename.substring(filename.lastIndexOf(StatReportLoader.XLS_TEMP_FILE_DELIMITER) + StatReportLoader.XLS_TEMP_FILE_DELIMITER.length(), filename.length());
    }

    public static String parseCellData(Cell cell) throws StatReportCellParseException {
        if (cell == null)
            return "";

        switch (cell.getCellType()) {
            case Cell.CELL_TYPE_STRING:
                return cell.getRichStringCellValue().getString();
            case Cell.CELL_TYPE_NUMERIC:
                if (DateUtil.isCellDateFormatted(cell))
                    return String.valueOf(cell.getDateCellValue().getTime());

                return new DecimalFormat("#.#").format(cell.getNumericCellValue());
            case Cell.CELL_TYPE_BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case Cell.CELL_TYPE_BLANK:
                return "";
            case Cell.CELL_TYPE_FORMULA:
                throw new StatReportCellParseException(cell.getColumnIndex(), "Unable to parse formula");
            default:
                throw new StatReportCellParseException(cell.getColumnIndex(), "Unknown cell type");
        }
    }
}
