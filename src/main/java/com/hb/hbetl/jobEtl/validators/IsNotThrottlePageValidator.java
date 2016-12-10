package com.hb.hbetl.jobEtl.validators;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.regex.Pattern;

public class IsNotThrottlePageValidator extends RuntimeException implements JobPageValidator {
    private static final long serialVersionUID = -22314248052553614L;

    private Pattern THROTTLE_PAGE_VALIDATOR = Pattern.compile(".*Due to the high demand it may take a little longer\\. You will be directed to the page shortly\\. Please do not leave this page\\. Refreshing the page will delay the response time\\. We apologize for the delay.*");

    @Override
    public void validate(Document jobDocument) throws JobPageValidationException {
        Elements paragraphElements = jobDocument.select("p");
        Element throttleMessageElement = paragraphElements.stream()
                .filter(Element::hasText)
                .filter(el -> THROTTLE_PAGE_VALIDATOR.matcher(el.text()).matches())
                .findFirst()
                .orElse(null);

        if (throttleMessageElement != null) {
            throw new JobPageValidationException("Throttle page");
        }
    }
}
