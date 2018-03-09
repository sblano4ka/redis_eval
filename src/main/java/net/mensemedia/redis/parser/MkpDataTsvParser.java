package net.mensemedia.redis.parser;

import net.mensemedia.redis.dto.Topic;
import net.mensemedia.redis.dto.TopicTranslation;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MkpDataTsvParser {

    private static final String SOURCE_LOCALE = "de_DE";
    private static final int DATA_WITHOUT_HEADERS_ROW_NUMBER = 1;
    private static final int UUID_ROWS_NUMBER = 1;
    private static final int HEADERS_ROW = 1;
    private static final int START_TRANSLATION_INDEX = 1;
    private static final int TOPIC_NAME_INDEX = 0;
    private static final int HEADER_ROW_INDEX = 0;

    public Set<Topic> parse(final InputStream inputStream) {
        return new HashSet(parseInputStream(inputStream));
    }

    protected List<Topic> parseInputStream(final InputStream content) {
        CSVParser parser = null;
        CSVFormat tdf = CSVFormat.TDF.withQuote(null);

        try {
            parser = getCsvParser(content, tdf);
            List<CSVRecord> records = parser.getRecords();
            if (records.isEmpty()) {
                return Collections.emptyList();
            }
            return processTsvRecords(records);
        } catch (IOException e) {
            System.out.println("Error here!");
        } finally {
            closeParser(parser);
        }
        return Collections.emptyList();
    }

    CSVParser getCsvParser(final InputStream content, final CSVFormat tdf) throws IOException {
        return new CSVParser(new InputStreamReader(content, StandardCharsets.UTF_16), tdf);
    }

    private List<Topic> processTsvRecords(final List<CSVRecord> records) {
        String[][] fileData = getInvertData(records);
        int topicsNumber = fileData.length - HEADERS_ROW;
        List<Topic> topics = new ArrayList<>(topicsNumber);
        List<String> translationHeaders = getTranslationHeaders(fileData[HEADER_ROW_INDEX]);
        for (int i = DATA_WITHOUT_HEADERS_ROW_NUMBER; i < topicsNumber; i++) {
            topics.add(getTopic(fileData[i], translationHeaders));
        }
        return topics;
    }

    private String[][] getInvertData(final List<CSVRecord> records) {
        String[][] inverted = new String[records.get(0).size()][records.size()];
        for (int i = 0; i < records.size(); i++) {
            for (int y = 0; y < records.get(i).size(); y++) {
                inverted[y][i] = records.get(i).get(y);
            }
        }
        return inverted;
    }

    private List<String> getTranslationHeaders(final String[] headerRowData) {
        List<String> headers = new ArrayList<>(Arrays.asList(headerRowData));
        headers.remove(HEADERS_ROW);
        headers.remove(headers.size() - UUID_ROWS_NUMBER);
        return headers;
    }

    private Topic getTopic(final String[] topicDataArray, final List<String> translationHeaders) {
        List<String> topicData = Arrays.asList(topicDataArray);
        List<TopicTranslation> topicTranslations = getTopicTranslations(translationHeaders, topicData);
        Topic topic = new Topic();
        topic.setTranslations(topicTranslations);
        topic.setName(topicData.get(TOPIC_NAME_INDEX));
        topic.setSourceContent(getSourceContent(topicTranslations));
        return topic;
    }

    private List<TopicTranslation> getTopicTranslations(final List<String> translationHeaders,
                                                        final List<String> topicData) {
        List<String> translations = getListWithTranslations(topicData);
        List<TopicTranslation> topicTranslations = new ArrayList<>(translations.size());
        for (int i = 0; i < translations.size(); i++) {
            topicTranslations.add(getTopicTranslation(translationHeaders.get(i), translations.get(i)));
        }
        return topicTranslations;
    }

    private List<String> getListWithTranslations(final List<String> topicList) {
        int endTranslationIndex = topicList.size() - START_TRANSLATION_INDEX;
        return topicList.subList(START_TRANSLATION_INDEX, endTranslationIndex);
    }

    private String getSourceContent(final List<TopicTranslation> topicTranslations) {
        return topicTranslations.stream()
                .filter(translation -> SOURCE_LOCALE.equals(translation.getLocale()))
                .map(TopicTranslation::getContent)
                .findFirst()
                .orElse(StringUtils.EMPTY);
    }

    private TopicTranslation getTopicTranslation(final String locale, final String translation) {
        TopicTranslation topicTranslation = new TopicTranslation();
        topicTranslation.setContent(translation);
        topicTranslation.setLocale(locale);
        return topicTranslation;
    }

    void closeParser(final Closeable parser) {
        if (parser != null) {
            try {
                parser.close();
            } catch (IOException e) {
            }
        }
    }
}
