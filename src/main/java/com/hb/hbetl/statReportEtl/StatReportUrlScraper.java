package com.hb.hbetl.statReportEtl;

import org.joda.time.DateTime;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Year;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Scrapes urls of weekly stat report excel files
 */
public class StatReportUrlScraper implements Callable<List<URL>> {
    private final List<Integer> yearsToScrapeReportsFor;
    private Collection<Exception> failures;
    private Collection<String> malformedLinks;

    public StatReportUrlScraper(List<Integer> yearsToScrapeReportsFor) {
        this.yearsToScrapeReportsFor = yearsToScrapeReportsFor;
    }

    public Collection<Exception> getFailures() {
        return failures;
    }

    public Collection<String> getMalformedLinks() {
        return malformedLinks;
    }

    public List<URL> scrapeJobStatFileLinksForYear(int year) {
        String url = String.format("https://www1.nyc.gov/site/buildings/about/job-statistical-reports-%d.page", year);
        String linkSelector = "a[href^=/assets/buildings/excel/job][href$=.xls]";
        if (year == Year.now().getValue())
            url = "http://www1.nyc.gov/site/buildings/about/job-statistical-reports.page";

        return scrapeLinkUrls(url, linkSelector);
    }

    public List<URL> scrapePermitStatFileLinksForYear(int year) {
        String url = String.format("https://www1.nyc.gov/site/buildings/about/permit-statistical-reports-%d.page", year);
        String linkSelector = "a[href^=/assets/buildings/excel/per][href$=.xls]";
        if (year == Year.now().getValue())
            url = "http://www1.nyc.gov/site/buildings/about/job-statistical-reports.page";

        return scrapeLinkUrls(url, linkSelector);
    }

    private List<URL> scrapeLinkUrls(String htmlPageUrl, String linkCssMatcher) {
        try {
            Document currentStatFilePage = Jsoup.connect(htmlPageUrl).get();
            Elements statFileLinks = currentStatFilePage.select(linkCssMatcher);

            return statFileLinks.stream()
                .map(this::parseUrlFromElement)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        } catch (IOException e) {
            this.failures.add(e);
        }

        return Collections.emptyList();
    }

    private URL parseUrlFromElement(Element element) {
        String href = element.absUrl("href");

        try {
            return new URL(href);
        } catch (MalformedURLException e) {
            this.malformedLinks.add(element.attr("href"));
        }

        return null;
    }

    @Override
    public List<URL> call() throws Exception {
        failures = new ConcurrentLinkedQueue<>();
        malformedLinks = new ConcurrentLinkedQueue<>();
        List<URL> statReportUrls = Collections.synchronizedList(new ArrayList<>());

        /*
        // TODO - Is there any info actually worth scrapping here?
        yearsToScrapeReportsFor.stream()
                .map(this::scrapePermitStatFileLinksForYear)
                .forEach(statReportUrls::addAll);
        */

        yearsToScrapeReportsFor.stream() // TODO - shall we parrallelize?
                .map(this::scrapeJobStatFileLinksForYear)
                .forEach(statReportUrls::addAll);

        return statReportUrls;
    }
}
