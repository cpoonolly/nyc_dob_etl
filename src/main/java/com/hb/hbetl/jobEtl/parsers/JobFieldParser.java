package com.hb.hbetl.jobEtl.parsers;

import org.jsoup.nodes.Document;

/**
 *
 */
public interface JobFieldParser {
    String getFieldName();
    String getSqlColumnDefinition();
    String parseFieldFromDocument(Document jobDocument) throws JobFieldParsingException;
}
