package net.lano4ka.redis.client;

import com.google.gson.Gson;
import net.lano4ka.redis.dto.TopicTranslation;
import net.lano4ka.redis.dto.Topic;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisConnectionHolder implements Client {

    private static JedisConnectionHolder HOLDER;
    private Jedis jedis;
    private Gson GSON = new Gson();

    public static JedisConnectionHolder getInstance() {
        if (HOLDER == null) {
            HOLDER = new JedisConnectionHolder();
        }
        return HOLDER;

    }

    @Override
    public void initConnection() {
        jedis = new Jedis("localhost");
        System.out.println("Connection to server sucessfully");
    }

    @Override
    public void closeConnection() {
        jedis.close();
        System.out.println("Connection closed");
    }

    @Override
    public void addString(final String key, final String val) {
        jedis.set(key, val);

        System.out.println("Added by key: " + key + "value: " + jedis.get(key));
    }

    @Override
    public void addStringList(final String key, final List<String> list) {
        for (String element : list) {
            jedis.rpush(key, element);
        }

        jedis.lrange(key, 0, list.size() - 1).forEach(el ->
                System.out.println("Stored list in redis by key '" + key + "' and value '" + el + "'"));
    }

    @Override
    public void addStringSet(final String key, final List<String> list) {
        list.forEach(element -> jedis.sadd(key, element));
        jedis.smembers(key)
                .forEach(el -> System.out.println("Set in redis by key '" + key + "' and value '" + el + "'"));


        System.out.println("Is 'Jack' member of set: " + jedis.sismember(key, "John"));
        System.out.println("Is 'Vasya' member of set: " + jedis.sismember(key, "Vasya"));
    }

    @Override
    public void addSortedSet(final String key, final Map<String, Double> scores, final String toTestKey) {
        scores.keySet().forEach(player ->
                jedis.zadd(key, scores.get(player), player)
        );

        System.out.println("Highest score: " + jedis.zrevrange(key, 0, 1).iterator().next());
        System.out.println(toTestKey + " has rank: " + jedis.zrevrank(key, toTestKey));
    }

    @Override
    public void transactionTest(List<Topic> topics) {
        Transaction transaction = jedis.multi();
        long start = System.currentTimeMillis();
        topics.forEach(topic -> transaction.set("topic" + topic.getName(), GSON.toJson(topic)));
        transaction.exec();
        System.out.println("Total time to put data to cache: " + (System.currentTimeMillis() - start));

        System.out.println("First topic: " + jedis.get("topic" + topics.get(0).getName()));
    }

    @Override
    public void publishSubscribing(final TopicTranslation topicTransaltion) {
        Thread subscriber = new Thread(() -> {
            Jedis jSubscriber = new Jedis();
            jSubscriber.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("JEDIS: I 've got new message her! Yeah Topic: " + message);
                }
            }, "JedisChannel");
        });
        subscriber.start();

        Thread publisher = new Thread(() -> {
            jedis.publish("JedisChannel", GSON.toJson(topicTransaltion));
        });
        publisher.start();
    }


    public void addHashTableValues(final String key, final TopicTranslation tr1, final TopicTranslation tr2) {
        addHash(key + "#1", tr1);
        addHash(key + "#2", tr2);

        jedis.hgetAll(key + "#1")
                .forEach((k, val) -> System.out.println("Hash value: " + k + " value: " + val));

        System.out.println("'Locale' value exists: " + jedis.hexists(key + "#1", "locale"));
        System.out.println("'Translation' value exists: " + jedis.hexists(key + "#1", "translation"));
    }

    private void addHash(final String key, final TopicTranslation tr1) {
        jedis.hset(key, "locale", tr1.getLocale());
        jedis.hset(key, "content", tr1.getContent());
    }



    @Override
    public void addSortedSet(final String key, final Set<Topic> scores, final String toTestKey) {

    }
}
