package net.mensemedia.redis.client;

import net.mensemedia.redis.dto.Topic;
import net.mensemedia.redis.dto.TopicTranslation;
import org.redisson.Redisson;
import org.redisson.api.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedissonConnectionHolder implements Client{

    private static RedissonConnectionHolder HOLDER;
    RedissonClient redisson;

    public static RedissonConnectionHolder getInstance() {
        if (HOLDER == null) {
            HOLDER = new RedissonConnectionHolder();
        }
        return HOLDER;

    }

    public void initConnection() {
//        Config config = new Config();
//        config.useSingleServer()
//                .setAddress("http://localhost:6379")
//                .setRetryAttempts(3)
//                .setConnectionPoolSize(5);
        redisson = Redisson.create();
        System.out.println("Connection to server sucessfully");
    }

    public void closeConnection() {
        redisson.shutdown();
        System.out.println("Connection closed");
    }

    public void addString(final String myStringKey, final String val) {
        redisson.getBucket(myStringKey).set(val);
        System.out.println("Added by key: " + myStringKey + "value: " + redisson.getBucket(myStringKey).get());
    }

    @Override
    public void addStringList(final String key, final List<String> list) {
        RList<String> list1 = redisson.getList(key);
        list1.addAll(list);
        list1.forEach(el ->
                System.out.println("Stored list in redis by key '" + key + "' and value '" + el + "'"));
    }

    @Override
    public void addStringSet(final String key, final List<String> list) {
        RSet<Object> set = redisson.getSet(key);
        set.addAll(list);
        set.forEach(el -> System.out.println("Set in redis by key '" + key + "' and value '" + el + "'"));


        System.out.println("Is 'Jack' member of set: " + set.contains("John"));
        System.out.println("Is 'Vasya' member of set: " + set.contains("Vasya"));
    }

    @Override
    public void addHashTableValues(final String key, final TopicTranslation tr1, final TopicTranslation tr2) {
        addHash(key + "#1", tr1);
        addHash(key + "#2", tr2);

        RMap<Object, Object> map1 = redisson.getMap(key + "#1");
        map1.forEach((k, val) -> System.out.println("Hash value: " + k + " value: " + val));

        System.out.println("'Locale' value exists: " + map1.containsKey("locale"));
        System.out.println("'Translation' value exists: " + map1.containsKey("translation"));
    }

    @Override
    public void publishSubscribing(final TopicTranslation topicTr) {
        Thread subscriber = new Thread(() -> {
            RTopic<TopicTranslation> topic = redisson.getTopic("redissonChanel");
            topic.addListener((channel, message) -> System.out.println("REDISSON: I 've got new message her! Yeah1/ Topic: " + message));
        });

        subscriber.start();

        Thread publisher = new Thread(() -> {
            RTopic<TopicTranslation> topic1 = redisson.getTopic("redissonChanel");
            topic1.publish(topicTr);
        });
        publisher.start();

    }
    private void addHash(final String key, final TopicTranslation tr1) {
        redisson.getMap(key).put("locale", tr1.getLocale());
        redisson.getMap(key).put("content", tr1.getContent());
    }

    @Override
    public void transactionTest(List<Topic> topics) {
    }


    @Override
    public void addSortedSet(final String key, final Map<String, Double> scores, final String toTestKey) {
    }

    @Override
    public void addSortedSet(final String key, final Set<Topic> scores, final String toTestKey) {
//        RSortedSet<Topic> sortedSet = redisson.getSortedSet(key);
//        sortedSet.trySetComparator(Comparator.comparing(Topic::getName));
//        sortedSet.addAll(scores);
//
//        System.out.println("Highest score: " + sortedSet.iterator().next());
    }
}
