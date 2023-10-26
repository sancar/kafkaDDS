package io.github.sancar.kafkadds.totalorderbrodacast;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryLog {

    private final Lock lock = new ReentrantLock();
    public final ArrayList<Message> log = new ArrayList<>();
    private final AtomicInteger offsetGenerator = new AtomicInteger(-1);

    public long offer(String key, String value, String header) {
        try {
            lock.lock();
            int offset = offsetGenerator.incrementAndGet();
            log.add(offset, new Message(offset, key, value, header));
            return offset;
        } finally {
            lock.unlock();
        }
    }

    public ArrayList<Message> consume(int consumeOffset) {
        try {
            lock.lock();
            ArrayList<Message> list = new ArrayList<>(log.size() - consumeOffset);
            for (; consumeOffset < log.size(); consumeOffset++) {
                list.add(log.get(consumeOffset));
            }
            return list;
        } finally {
            lock.unlock();
        }
    }

}
