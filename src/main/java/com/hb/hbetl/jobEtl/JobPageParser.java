package com.hb.hbetl.jobEtl;

import com.google.common.collect.ImmutableList;
import com.hb.hbetl.jobEtl.parsers.JobFieldParser;
import com.hb.hbetl.jobEtl.parsers.JobFieldParsingException;
import com.hb.hbetl.jobEtl.parsers.OwnerEmailFieldParser;
import com.hb.hbetl.jobEtl.validators.IsNotThrottlePageValidator;
import com.hb.hbetl.jobEtl.validators.JobPageValidationException;
import com.hb.hbetl.jobEtl.validators.JobPageValidator;
import org.jsoup.nodes.Document;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobPageParser {
    private static final String TABLE_NAME = "job_parsed";
    private static final List<JobPageValidator> VALIDATORS = ImmutableList.of(new IsNotThrottlePageValidator());
    private static final List<JobFieldParser> FIELD_PARSERS = ImmutableList.of(new OwnerEmailFieldParser());

    public final Integer jobNumber;
    private final Document jobDocument;
    private final Map<JobFieldParser, String> parsedValues;

    public JobPageParser(Integer jobNumber, Document jobDocument) throws JobPageValidationException, JobFieldParsingException {
        this.jobNumber = jobNumber;
        this.jobDocument = jobDocument;
        this.parsedValues = new HashMap<>(FIELD_PARSERS.size());

        for (JobPageValidator validator : VALIDATORS) {
            validator.validate(this.jobDocument);
        }

        for (JobFieldParser fieldParser : FIELD_PARSERS) {
            parsedValues.put(fieldParser, fieldParser.parseFieldFromDocument(this.jobDocument));
        }
    }

    private String generateInsertValuesSql() {
        String sql = "(" + jobNumber + ",'";
        sql += FIELD_PARSERS.stream().map(parsedValues::get).collect(Collectors.joining("', '"));
        sql += "')";

        return sql;
    }

    public static String generateInsertSql(JobPageParser parser) {
        return generateInsertSql(Collections.singletonList(parser));
    }

    public static String generateInsertSql(List<JobPageParser> parsers) {
        String insertSql = "insert into " + TABLE_NAME + "(job_num, ";
        insertSql += FIELD_PARSERS.stream().map(JobFieldParser::getFieldName).collect(Collectors.joining(", "));
        insertSql += ") values ";
        insertSql += parsers.stream().map(JobPageParser::generateInsertValuesSql).collect(Collectors.joining(", "));

        return insertSql;
    }

    public static String getCreateTableSql() {
        String columnDefinitions = FIELD_PARSERS.stream()
                .map(JobFieldParser::getSqlColumnDefinition)
                .collect(Collectors.joining(", "));

        return "create table if not exists " + TABLE_NAME + " (job_num integer not null, " + columnDefinitions + ")";
    }
}
