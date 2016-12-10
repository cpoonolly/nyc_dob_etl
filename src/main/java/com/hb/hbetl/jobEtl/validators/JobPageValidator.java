package com.hb.hbetl.jobEtl.validators;

import org.jsoup.nodes.Document;

/**
 * Validates an scraped job html page from the dob
 */
public interface JobPageValidator {
    public void validate(Document jobPageDocument) throws JobPageValidationException;
}
