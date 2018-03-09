package net.lano4ka.redis.dto;

import org.apache.commons.lang3.StringUtils;
import java.util.List;

public class Topic {
    private static final String SOURCE_LOCALE = "de_DE";
    private String name;
    private String sourceContent;
    private List<TopicTranslation> translations;

    public Topic() {
    }

    public Topic(final String name, final String sourceContent) {
        this.name = name;
        this.sourceContent = sourceContent;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setSourceContent(final String sourceContent) {
        this.sourceContent = sourceContent;
    }

    public String getSourceContent() {
        return sourceContent;
    }

    public void setTranslations(final List<TopicTranslation> translations) {
        this.translations = translations;
    }

    public String getTranslation(final String locale) {
        if (SOURCE_LOCALE.equals(locale)) {
            return sourceContent;
        }

        return translations.stream()
                .filter(topicTranslation -> locale.equals(topicTranslation.getLocale()))
                .findFirst()
                .map(TopicTranslation::getContent)
                .orElse(StringUtils.EMPTY);
    }
}
