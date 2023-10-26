package io.github.sancar.kafkadds;

import org.junit.Test;
import io.github.sancar.kafkadds.totalorderbrodacast.InMemoryLog;
import io.github.sancar.kafkadds.totalorderbrodacast.InmemoryTotalOrderBroadcast;
import io.github.sancar.kafkadds.totalorderbrodacast.Message;
import io.github.sancar.kafkadds.totalorderbrodacast.TotalOrderBroadcast;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TotalOrderBroadcastTest {

    @Test
    public void testBasic() throws InterruptedException {
        InmemoryTotalOrderBroadcast totalOrderBroadcast = new InmemoryTotalOrderBroadcast(new InMemoryLog());
        testBasic(totalOrderBroadcast);
    }

    public void testBasic(TotalOrderBroadcast totalOrderBroadcast) throws InterruptedException {
        Random random = new Random();
        Map<Long, String> messages = new ConcurrentHashMap<>();

        for (int i = 0; i < 100; i++) {
            String v = "message-" + random.nextLong();
            long offset = totalOrderBroadcast.offer("key", v, "header");
            messages.put(offset, v);
        }

        Collection<Message> consume = totalOrderBroadcast.consume();

        assertEquals(100, consume.size());

        consume.forEach(message -> assertEquals(message.value(), messages.get(message.offset())));

        for (int i = 0; i < 50; i++) {
            String v = "message-" + random.nextLong();
            long offset = totalOrderBroadcast.offer("key", v, "header");
            String put = messages.put(offset, v);
            assertNull(put);
        }

        consume = totalOrderBroadcast.consume();
        assertEquals(150, consume.size());
        consume.forEach(message -> assertEquals(message.value(), messages.get(message.offset())));


        consume = totalOrderBroadcast.consume();
        assertEquals(100, consume.size());
    }
}
