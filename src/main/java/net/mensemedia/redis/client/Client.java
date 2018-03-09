package net.mensemedia.redis.client;

import net.mensemedia.redis.dto.Topic;
import net.mensemedia.redis.dto.TopicTranslation;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Client {
    void initConnection();

    void addString(String myStringKey, String s);

    void addStringSet(String myNamesSet, List<String> namesList);

    void addSortedSet(final String key, final Map<String, Double> scores, final String toTestKey);

    void addSortedSet(final String key, final Set<Topic> scores, final String toTestKey);

    void addHashTableValues(String translations, TopicTranslation de_de, TopicTranslation en_gb);

    void transactionTest(List<Topic> topics);

    void closeConnection();

    void addStringList(String key, List<String> namesList);

    void publishSubscribing(final TopicTranslation de_de);
}
