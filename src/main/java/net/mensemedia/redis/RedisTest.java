package net.mensemedia.redis;

import net.mensemedia.redis.client.Client;
import net.mensemedia.redis.client.JedisConnectionHolder;
import net.mensemedia.redis.client.RedissonConnectionHolder;
import net.mensemedia.redis.dto.Topic;
import net.mensemedia.redis.dto.TopicTranslation;
import net.mensemedia.redis.parser.MkpDataTsvParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class RedisTest {

    public static List<String> namesList = new ArrayList<>();

    {
        namesList.add("John");
        namesList.add("Jeremy");
        namesList.add("Clark");
    }

    public void test() throws FileNotFoundException, InterruptedException {

        JedisConnectionHolder holder = JedisConnectionHolder.getInstance();
        testWithClient(holder, "jedis");

        RedissonConnectionHolder redissonConnectionHolder = RedissonConnectionHolder.getInstance();
        testWithClient(redissonConnectionHolder, "redisson");
    }

    private void testWithClient(final Client holder, final String prefix) throws FileNotFoundException, InterruptedException {
        MkpDataTsvParser parser = new MkpDataTsvParser();
        Set<Topic> topics = parser.parse(new FileInputStream(new File("src/main/resources/list2.txt")));


        holder.initConnection();

        testString(holder, prefix);
        testList(holder, prefix);
        testSet(holder, prefix);
        testSortedSet(holder, prefix, topics);
        testHash(holder, prefix);
        testTransaction(holder, topics);
        testPubSub(holder);

        Thread.sleep(300);
        holder.closeConnection();
    }

    private void testPubSub(final Client holder) {
        System.out.println("=== Publish/Subscriber ============");
        holder.publishSubscribing(new TopicTranslation("de_DE", "De translation"));
    }

    private void testTransaction(final Client holder, final Set<Topic> topics) throws FileNotFoundException {

        holder.transactionTest(new ArrayList<Topic>(topics));
    }

    private void testHash(final Client holder, final String prefix) {
        System.out.println("=== HASH ============");

        TopicTranslation de_de = new TopicTranslation("de_DE", "Some german translation");
        TopicTranslation en_GB = new TopicTranslation("en_GB", "Some english translation");
        holder.addHashTableValues("translations" + prefix, de_de, en_GB);
        System.out.println("=== Transaction ============");
    }

    private void testSortedSet(final Client holder, final String prefix, final Set<Topic> topics) {
        System.out.println("=== SORTED SET ============");
        Map<String, Double> scores = new HashMap<>();
        scores.put("PlayerOne", 3000.0);
        scores.put("PlayerTwo", 1500.0);
        scores.put("PlayerThree", 8200.0);
        holder.addSortedSet("players" + prefix, scores, "PlayerOne");


        holder.addSortedSet("players" + prefix, topics, "PlayerOne");
    }

    private void testSet(final Client holder, final String prefix) {
        holder.addStringSet("myNamesSet" + prefix, namesList);
    }

    private void testList(final Client holder, final String prefix) {
        listTest(holder, prefix);
        System.out.println("=== SET ============");
    }

    private void testString(final Client holder, final String prefix) {
        holder.addString("myStringKey" + prefix, "My String value");
        System.out.println("===============");
    }

    private void listTest(final Client holder, final String prefix) {
        holder.addStringList("myNamesListSortedFromTheEnd" + prefix, namesList);
        System.out.println("===");
        holder.addStringList("myNamesListSorted" +prefix, namesList);
    }
}
