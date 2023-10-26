package io.github.sancar.kafkadds;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.Test;
import io.github.sancar.kafkadds.linearizable.Linearizable;
import io.github.sancar.kafkadds.linearizable.ReplicatedMap;
import io.github.sancar.kafkadds.totalorderbrodacast.InMemoryLog;
import io.github.sancar.kafkadds.totalorderbrodacast.InmemoryTotalOrderBroadcast;
import io.github.sancar.kafkadds.totalorderbrodacast.Records;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;
import static io.github.sancar.kafkadds.util.Json.toJson;
import static io.github.sancar.kafkadds.util.Json.toObject;

public class ReplicatedMapTest {

    // ENTER UPSTASH CREDENTIALS HERE to make tests pass
    private static final String bootstrap = "127.0.0.1:19091";//upstash
    private static final String jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ups-test\" password=\"123456\";";

    @Test
    public void TestSingleThread_ConcurrentHashMap() {
        // Just to check that test is correct
        testSingleThread(new ConcurrentHashMap<>());
    }

    @Test
    public void TestSingleThread_InMemory() {
        try (ReplicatedMap map = new ReplicatedMap(new InmemoryTotalOrderBroadcast(new InMemoryLog()))) {
            testSingleThread(map);
        }
    }

    @Test
    public void TestSingleThread_OnKafka() {
        Properties props = new Properties();
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.jaas.config", jaas);
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (ReplicatedMap map = Linearizable.newMap(props)) {
            testSingleThread(map);
        }
    }

    private static void testSingleThread(ConcurrentMap<String, String> map) {
        assertNull(map.remove("foo"));
        assertNull(map.get("foo"));
        assertNull(map.put("foo", "bar"));
        assertEquals("bar", map.remove("foo"));

        assertNull(map.putIfAbsent("foo", "bar"));
        assertEquals("bar", map.get("foo"));

        assertEquals("bar", map.putIfAbsent("foo", "hello"));

        assertEquals("bar", map.put("foo", "secret"));
        assertEquals("secret", map.get("foo"));
        assertFalse(map.replace("foo", "bar", "world"));
        assertTrue(map.replace("foo", "secret", "world"));
        assertEquals("world", map.get("foo"));
        assertEquals("world", map.remove("foo"));

        assertNull(map.replace("foo", "value"));
        assertNull(map.put("foo", "gold"));
        assertEquals("gold", map.replace("foo", "value"));
        assertEquals("value", map.remove("foo"));
    }

    @Test
    public void TestMultiNode_ConcurrentHashMap() throws InterruptedException {
        // Just to check that test is correct
        var m = new ConcurrentHashMap<String, String>();
        testMulti(m, m);
    }

    @Test
    public void TestMultiNode_InMemory() throws InterruptedException {
        InMemoryLog log = new InMemoryLog();
        ReplicatedMap map1 = new ReplicatedMap(new InmemoryTotalOrderBroadcast(log));
        ReplicatedMap map2 = new ReplicatedMap(new InmemoryTotalOrderBroadcast(log));
        testMulti(map1, map2);
    }

    @Test
    public void TestMultiThreadSingleNode_InMemory() throws InterruptedException {
        InMemoryLog log = new InMemoryLog();
        ReplicatedMap map1 = new ReplicatedMap(new InmemoryTotalOrderBroadcast(log));
        testMulti(map1, map1);
        map1.close();
    }

    @Test
    public void TestMultiNode_OnKafka() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.jaas.config", jaas);
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        ReplicatedMap m1 = Linearizable.newMap(props);
        ReplicatedMap m2 = Linearizable.newMap(props);
        testMulti(m1, m2);
        m1.close();
        m2.close();
    }

    @Test
    public void TestMultiThreadSingleNode_OnKafka() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.jaas.config", jaas);
        props.setProperty("bootstrap.servers", bootstrap);

        ReplicatedMap m1 = Linearizable.newMap(props);
        testMulti(m1, m1);
        m1.close();
    }

    private static void testMulti(ConcurrentMap<String, String> map1, ConcurrentMap<String, String> map2) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(6);
        new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                map1.put(String.valueOf(i), "map1:" + i);
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                map2.put(String.valueOf(i), "map2:" + i);
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 30; i++) {
                map2.remove(String.valueOf(i));
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 30; i++) {
                map1.remove(String.valueOf(i));
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 40; i++) {
                map1.putIfAbsent(String.valueOf(i), "map1Absent:" + i);
            }
            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 40; i++) {
                map2.putIfAbsent(String.valueOf(i), "map2Absent:" + i);
            }
            latch.countDown();
        }).start();

        latch.await();

        assertEquals(map1.size(), map2.size());
        for (int i = 0; i < 50; i++) {
//            System.out.println(map1.get(String.valueOf(i)));
            assertEquals(map1.get(String.valueOf(i)), map2.get(String.valueOf(i)));
        }
    }

}
