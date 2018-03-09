package net.mensemedia.redis.dto;

public class TopicTranslation {
    private String content;
    private String locale;

    public TopicTranslation() {
    }

    public TopicTranslation(final String locale, final String content) {
        this.content = content;
        this.locale = locale;
    }

    public String getContent() {
        return content;
    }

    public void setContent(final String content) {
        this.content = content;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(final String locale) {
        this.locale = locale;
    }
}
