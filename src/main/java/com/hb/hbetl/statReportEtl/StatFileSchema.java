package com.hb.hbetl.statReportEtl;

import com.hb.hbetl.statReportEtl.exceptions.InvalidStatSchemaException;
import com.hb.hbetl.statReportEtl.exceptions.StatReportCellParseException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class StatFileSchema {
    public static final String FILE_REF_COLUMN_NAME = "file_ref";

    private static Map<StatFileType, StatFileSchema> instances = new EnumMap<>(StatFileType.class);

    // we want AbstractList's implementation of .equals() (it returns true if .equals() == true for all elements)
    private AbstractList<StatFileColumnSchema> columnSchemas;

    private final StatFileType fileType;

    private StatFileSchema(StatFileType fileType) throws IOException {
        this.fileType = fileType;

        if (fileType.sampleFile == null) {
            this.columnSchemas = new ArrayList<>();
            return;
        }

        InputStream sampleFileInputStream = getClass().getResourceAsStream(fileType.sampleFile);
        try (NPOIFSFileSystem poiFileSystem = new NPOIFSFileSystem(sampleFileInputStream)) {
            HSSFWorkbook workbook = new HSSFWorkbook(poiFileSystem.getRoot(), false);
            HSSFSheet sheet = workbook.getSheetAt(0);

            this.columnSchemas = extractColumnSchemas(sheet);
        } catch (StatReportCellParseException e) {
            throw new IOException("Failed to load validator for fileType: " + fileType.name());
        }
    }

    public String getSqlTableDefinition() {
        String tableName = fileType.sqlTableName;
        String columnDefSql = columnSchemas.stream()
                .map(StatFileColumnSchema::getSqlColumnDefinition)
                .collect(Collectors.joining(", "));

        columnDefSql += String.format(", %s varchar(50)", FILE_REF_COLUMN_NAME);
        return String.format("create table if not exists %s (%s)", tableName, columnDefSql);
    }

    public List<StatFileColumnSchema> getColumnSchemas() {
        return Collections.unmodifiableList(columnSchemas);
    }

    public void validateSchema(HSSFSheet sheet, String filename) throws InvalidStatSchemaException {
        try {
            AbstractList<StatFileColumnSchema> schemas = extractColumnSchemas(sheet);

            if (!columnSchemas.equals(schemas))
                throw new InvalidStatSchemaException(filename);

        } catch (StatReportCellParseException e) {
            throw new InvalidStatSchemaException(filename, e);
        }
    }

    private AbstractList<StatFileColumnSchema> extractColumnSchemas(HSSFSheet sheet)
            throws StatReportCellParseException {

        Row headerRow = sheet.getRow(fileType.headerRowNum - 1);

        AbstractList<StatFileColumnSchema> schemas = new ArrayList<>();
        for (Cell cell : headerRow) {
            StatFileColumnSchema columnSchema = new StatFileColumnSchema(cell, sheet, fileType);
            if (columnSchema.headerColumnName == null || columnSchema.headerColumnName.isEmpty())
                continue;

            schemas.add(columnSchema);
        }

        return schemas;
    }

    public static StatFileSchema getSchemaForFileType(StatFileType fileType) throws IOException {
        if (!instances.containsKey(fileType)) {
            instances.put(fileType, new StatFileSchema(fileType));
        }

        return instances.get(fileType);
    }

}
