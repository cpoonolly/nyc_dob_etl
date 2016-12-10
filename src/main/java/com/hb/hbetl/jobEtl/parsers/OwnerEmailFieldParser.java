package com.hb.hbetl.jobEtl.parsers;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.regex.Pattern;

/**
 * Parses Owner email field from the job page
 */
public class OwnerEmailFieldParser implements JobFieldParser {
    private static final Pattern OWNER_INFO_HEADER_PATTERN = Pattern.compile("26.*Owner's Information");
    private static final Pattern OWNER_EMAIL_LABEL_PATTERN = Pattern.compile(".*E-Mail:.*");
    private static final Pattern VALID_EMAIL_PATTERN = Pattern.compile("^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$");

    @Override
    public String getFieldName() {
        return "owner_email";
    }

    @Override
    public String getSqlColumnDefinition() {
        return "owner_email varchar(50)";
    }

    @Override
    public String parseFieldFromDocument(Document jobDocument) throws JobFieldParsingException {
        Elements elements = jobDocument.select("table tbody tr td.colhdg");
        if (elements == null || elements.size() < 1) {
            throw new JobFieldParsingException(getFieldName(), "failed to find sections");
        }

        Element ownerInfoSectionHeader = elements.stream()
                .filter(Element::hasText)
                .filter(el -> OWNER_INFO_HEADER_PATTERN.matcher(el.text()).matches())
                .findFirst()
                .orElse(null);

        if (ownerInfoSectionHeader == null) {
            throw new JobFieldParsingException(getFieldName(), "failed to find owner info section");
        }

        Element ownerInfoSection = ownerInfoSectionHeader.parent().parent().parent();
        Element ownerEmailLabel = ownerInfoSection.select("tr td.rightlabel").stream()
                .filter(Element::hasText)
                .filter(el -> OWNER_EMAIL_LABEL_PATTERN.matcher(el.text()).matches())
                .findFirst()
                .orElse(null);

        if (ownerEmailLabel == null) {
            throw new JobFieldParsingException(getFieldName(), "couldn't find owner email field");
        }

        Element ownerEmail = ownerEmailLabel.nextElementSibling();
        if (ownerEmail == null) {
            throw new JobFieldParsingException(getFieldName(), "No owner email found");
        } else if (!ownerEmail.hasClass("content")) {
            throw new JobFieldParsingException(getFieldName(), "Invalid owner email element - no content class found");
        } else if (ownerEmail.text() != null && !VALID_EMAIL_PATTERN.matcher(ownerEmail.text()).matches()) {
            throw new JobFieldParsingException(getFieldName(), "Owner email does not match valid email format");
        }

        return ownerEmail.text();
    }
}
