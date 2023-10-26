package io.github.sancar.kafkadds.totalorderbrodacast;

import java.util.ArrayList;
import java.util.Collection;

// Just for testing the real implementation is on Kafka. Not thread safe.
public class InmemoryTotalOrderBroadcast implements TotalOrderBroadcast {

    private final InMemoryLog log;
    private int consumeOffset = 0;

    private final Runnable callback;

    // Callback to be called before consume to create artificial latency for tests
    public InmemoryTotalOrderBroadcast(InMemoryLog log, Runnable callback) {
        this.log = log;
        this.callback = callback;
    }

    public InmemoryTotalOrderBroadcast(InMemoryLog log) {
        this.log = log;
        this.callback = () -> {
        };
    }

    @Override
    public long offer(String key, String value, String header) {
        return log.offer(key, value, header);
    }

    @Override
    public Collection<Message> consume() {
        callback.run();
        while (true) {
            ArrayList<Message> messages = log.consume(consumeOffset);
            if (!messages.isEmpty()) {
                Message message = messages.get(messages.size() - 1);
                consumeOffset = (int) message.offset();
                return messages;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
