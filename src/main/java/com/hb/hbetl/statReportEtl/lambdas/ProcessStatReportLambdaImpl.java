package com.hb.hbetl.statReportEtl.lambdas;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.hb.hbetl.HbEtl;
import com.hb.hbetl.statReportEtl.StatFileColumnSchema;
import com.hb.hbetl.statReportEtl.StatFileSchema;
import com.hb.hbetl.statReportEtl.StatFileType;
import com.hb.hbetl.statReportEtl.StatReportLoader;
import com.hb.hbetl.statReportEtl.exceptions.StatFileProcessingException;
import com.hb.hbetl.statReportEtl.exceptions.StatReportCellParseException;
import com.hb.hbetl.statReportEtl.exceptions.StatReportRowParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hb.hbetl.statReportEtl.StatReportProcessor.*;

/**
 * Implementation of {@link ProcessStatReportLambda}
 */
public class ProcessStatReportLambdaImpl implements ProcessStatReportLambda {
    private final AmazonS3 s3;
    private final AtomicInteger parsedRowCount;
    private final List<StatReportRowParseException> parsingFailures;

    public ProcessStatReportLambdaImpl() {
        this.s3 = AmazonS3ClientBuilder.defaultClient();
        this.parsedRowCount = new AtomicInteger();
        this.parsingFailures = new ArrayList<>();
    }

    @Override
    public ProcessStatReportResult processS3StatReport(String statReportXlsS3Key) {
        try {
            File statFile = loadFileFromS3(statReportXlsS3Key);
            String csvS3Key = processXlsStatReport(statFile);

            return new ProcessStatReportResult(csvS3Key, parsedRowCount.intValue(), parsingFailures);
        } catch (IOException e) {
            throw new StatFileProcessingException(HbEtl.getFilenameForS3Key(statReportXlsS3Key), e);
        }
    }

    public String processXlsStatReport(File statReport) throws IOException {
        String csvFilename = getCsvFilenameForStatReport(statReport);
        String csvS3Key = getCsvS3KeyForStatReport(statReport);

        // Convert xls stat report to a csv
        File csvFile = File.createTempFile(CSV_TEMP_FILE_PREFIX, CSV_TEMP_FILE_DELIMITER + csvFilename);
        convertXlsStatFileToCsv(statReport, csvFile);

        // Put csv into s3
        s3.putObject(HbEtl.S3_BUCKET, csvS3Key, csvFile);

        return csvS3Key;
    }

    private void convertXlsStatFileToCsv(File statReportXls, File outputCsv) throws IOException {
        String statFileName = parseOriginalStatReportFilename(statReportXls);
        StatFileType fileType = StatFileType.getFileTypeFromFilename(statFileName);
        StatFileSchema statFileSchema = StatFileSchema.getSchemaForFileType(fileType);

        try (
            NPOIFSFileSystem poiFileSystem = new NPOIFSFileSystem(statReportXls);
            PrintWriter csvOutput = new PrintWriter(outputCsv)
        ) {
            HSSFWorkbook workbook = new HSSFWorkbook(poiFileSystem.getRoot(), false);
            HSSFSheet sheet = workbook.getSheetAt(0);
            statFileSchema.validateSchema(sheet, statFileName);

            int rowNum = 0;
            List<StatFileColumnSchema> columnSchemas = statFileSchema.getColumnSchemas();
            for (Row row : sheet) {
                rowNum++; // xls rows start at 1

                // Skip header row
                if (rowNum <= fileType.headerRowNum)
                    continue;

                try {
                    String csvRow = generateCsvRow(row, columnSchemas, statFileName);
                    csvOutput.print(csvRow);
                    parsedRowCount.incrementAndGet();
                } catch (StatReportCellParseException e) {
                    parsingFailures.add(new StatReportRowParseException(statFileName, rowNum, e));
                }
            }
        }
    }

    private String generateCsvRow(Row row, List<StatFileColumnSchema> columnSchemas, String statFileName)
            throws StatReportCellParseException {

        StringBuilder csvRow = new StringBuilder();
        int colNum = 0;

        // Parse cell data and write it to the csv output
        for (StatFileColumnSchema columnSchema : columnSchemas) {
            Cell cell = row.getCell(colNum);

            String cellData = parseCellData(cell);
            columnSchema.validateCellSchema(cellData, colNum);

            csvRow.append(cellData);
            csvRow.append(CSV_VALUE_DELIMITER);

            colNum++;
        }

        // Last column is the statFileName
        csvRow.append(FilenameUtils.getBaseName(statFileName));
        csvRow.append('\n');

        return csvRow.toString();
    }

    private File loadFileFromS3(String s3Key) throws IOException {
        S3Object s3Obj = s3.getObject(HbEtl.S3_BUCKET, s3Key);
        String filename = HbEtl.getFilenameForS3Key(s3Key);

        try (InputStream s3ObjectInputStream = s3Obj.getObjectContent()) {
            File statReportFile = File.createTempFile(StatReportLoader.XLS_TEMP_FILE_PREFIX, StatReportLoader.XLS_TEMP_FILE_DELIMITER + filename);
            FileUtils.copyInputStreamToFile(s3ObjectInputStream, statReportFile);

            return statReportFile;
        }
    }
}
