package com.hb.hbetl.statReportEtl;

import com.hb.hbetl.statReportEtl.exceptions.StatReportCellParseException;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.util.regex.Pattern;

import static com.hb.hbetl.statReportEtl.StatReportProcessor.CSV_VALUE_DELIMITER;

/**
 *
 */
public final class StatFileColumnSchema {
    private static final Pattern CELL_EXCLUDES_DELIMITER_PATTERN = Pattern.compile("[^" + CSV_VALUE_DELIMITER + "]*");
    private static final Pattern VALID_PRIMARY_KEY_PATTERN = Pattern.compile("[0-9]+");
    private static final Pattern VALID_NUMERIC_CELL_PATTERN = Pattern.compile("[0-9\\\\.\\\\-]*");

    public final String headerColumnName;
    private final int headerColumnType;
    private final int precision;
    private final StatFileType fileType;
//    private final boolean nullable;
//    private final boolean isDate;

    public StatFileColumnSchema(Cell headerColumnCell, HSSFSheet sheet, StatFileType fileType) throws StatReportCellParseException {
        this.headerColumnName = StatReportProcessor.parseCellData(headerColumnCell);
        this.headerColumnType = headerColumnCell.getCellType();
        this.fileType = fileType;
//        this.isDate = DateUtil.isCellDateFormatted(headerColumnCell);

        if (headerColumnType == Cell.CELL_TYPE_STRING) {
            precision = computeCharFieldPrecision(headerColumnCell, sheet, fileType);
        } else {
            precision = 0;
        }
    }

    public void validateCellSchema(String cellData, Integer columnNum) throws StatReportCellParseException {
        // Validate primary key
        if (columnNum == fileType.primaryKeyColumnIndex) {
            if (!VALID_PRIMARY_KEY_PATTERN.matcher(cellData).matches())
                throw new StatReportCellParseException(columnNum, "Invalid primaryKey. Cell value:'" + cellData + "'");
        }

        // make sure the cell doesn't contain the delimitor
        if (!CELL_EXCLUDES_DELIMITER_PATTERN.matcher(cellData).matches())
            throw new StatReportCellParseException(columnNum, "Cell contains delimiter. Cell value:'" + cellData + "'");

        // validate
        switch (headerColumnType) {
            case Cell.CELL_TYPE_STRING:
                if (cellData.length() > precision)
                    throw new StatReportCellParseException(columnNum, "Invalid column precision. Cell value:'" + cellData + "' Exceeded expected precision:" + precision);
                break;
            case Cell.CELL_TYPE_NUMERIC:
                if (VALID_NUMERIC_CELL_PATTERN.matcher(cellData).matches())
                    throw new StatReportCellParseException(columnNum, "Invalid numeric cell. Cell value:'" + cellData + "'");
            default:
                break;
        }
    }

    public String getSqlColumnDefinition() {
        return String.format("%s %s", getSqlColumnName(), getSqlDataType());
    }

    public String getSqlColumnName() {
        return headerColumnName
                .replaceAll("#", "num")
                .replaceAll("[^a-zA-z0-9\\s]", "")
                .replaceAll("\\s", "_")
                .toLowerCase();
    }

    private String getSqlDataType() {
        switch (headerColumnType) {
            case Cell.CELL_TYPE_STRING:
                if (precision < 0)
                    return "VARCHAR(MAX)";

                return String.format("VARCHAR(%d)", precision);
            case Cell.CELL_TYPE_NUMERIC:
                return "INTEGER"; // TODO - Handle dates here as well
            case Cell.CELL_TYPE_BOOLEAN:
                return "BOOLEAN";
            default:
                return "";
        }
    }

    private int computeCharFieldPrecision(Cell headerColumnCell, HSSFSheet sheet, StatFileType fileType)
            throws StatReportCellParseException {
        int maxStrLength = 0;
        int rowNum = 0;

        for (Row row : sheet) {
            rowNum++;
            if (rowNum <= fileType.headerRowNum)
                continue;;

            Cell cell = row.getCell(headerColumnCell.getColumnIndex());
            if (cell == null)
                continue;;

            String val = StatReportProcessor.parseCellData(cell);
            if (val != null && val.length() > maxStrLength) {
                if (maxStrLength < 32) maxStrLength = 32;
                else if (maxStrLength < 100) maxStrLength = 100;
                else if (maxStrLength < 500) maxStrLength = 500;
                else maxStrLength = -1;
            }
        }

        return maxStrLength;
    }

    @Override
    public int hashCode() {
        return (headerColumnName + headerColumnType).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StatFileColumnSchema) {
            return headerColumnName.equals(((StatFileColumnSchema) obj).headerColumnName)
                    && headerColumnType == ((StatFileColumnSchema) obj).headerColumnType;
        }

        return super.equals(obj);
    }
}
