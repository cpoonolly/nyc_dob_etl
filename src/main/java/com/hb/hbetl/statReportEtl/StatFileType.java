package com.hb.hbetl.statReportEtl;

import com.hb.hbetl.statReportEtl.exceptions.InvalidStatFileException;

/**
 *
 */
public enum StatFileType {
    JOB(0, 3, "job_stat", "/sampleSchemas/sampleJobSchema.xls"), // TODO - Make this sample file muuuuuuuch smaller
    PERMIT(4, 3, "permit_stat", null);

    public final int primaryKeyColumnIndex;
    public final int headerRowNum;
    public final String sqlTableName;
    public final String sampleFile;

    StatFileType(int primaryKeyColumnIndex, int headerRowNum, String sqlTableName, String sampleFile) {
        this.primaryKeyColumnIndex = primaryKeyColumnIndex;
        this.headerRowNum = headerRowNum;
        this.sqlTableName = sqlTableName;
        this.sampleFile = sampleFile;
    }

    public static StatFileType getFileTypeFromFilename(String filename) throws InvalidStatFileException {
        if (filename.startsWith("job"))
            return JOB;
        if (filename.startsWith("per"))
            return PERMIT;

        throw new InvalidStatFileException(filename);
    }
}
