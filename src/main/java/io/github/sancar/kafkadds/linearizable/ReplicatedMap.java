package io.github.sancar.kafkadds.linearizable;


import io.github.sancar.kafkadds.totalorderbrodacast.Message;
import io.github.sancar.kafkadds.totalorderbrodacast.Records;
import io.github.sancar.kafkadds.totalorderbrodacast.TotalOrderBroadcast;
import io.github.sancar.kafkadds.util.ValueBarrier;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static io.github.sancar.kafkadds.util.Json.toJson;
import static io.github.sancar.kafkadds.util.Json.toObject;


public class ReplicatedMap implements ConcurrentMap<String, String>, Closeable {

    public record VersionedValue(int version, String value) {
    }

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    // This lock is make sure that the data map and wait map is updated/read atomically
    private final Lock lock = new ReentrantLock();
    private final ValueBarrier lastUpdateBarrier = new ValueBarrier();
    private final TotalOrderBroadcast totalOrderBroadcast;
    private final HashMap<String, List<CompletableFuture<VersionedValue>>> waitMap = new HashMap<>();
    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();

    public ReplicatedMap(TotalOrderBroadcast totalOrderBroadcast) {
        this.totalOrderBroadcast = totalOrderBroadcast;
        new Thread(() -> {
            while (isRunning.get()) {
                Collection<Message> messages = totalOrderBroadcast.consume();

                messages.forEach(message -> {
                    if (Records.HeaderValues.WRITE_ATTEMPT.equals(message.op())) {
                        try {
                            lock.lock();
                            Records.WriteAttemptKey attempt = toObject(message.key(), Records.WriteAttemptKey.class);
                            VersionedValue old = data.get(attempt.key());
                            int existingVersion = 0;
                            if (old != null) {
                                existingVersion = old.version;
                            }

                            Records.WriteAttemptValue value = toObject(message.value(), Records.WriteAttemptValue.class);
                            var newValue = new VersionedValue(value.version(), value.value());
                            if (value.version() > existingVersion) {
                                data.put(attempt.key(), newValue);
                            }

                            List<CompletableFuture<VersionedValue>> futures = waitMap.remove(attempt.key() + ":" + value.version());
                            if (futures != null) {
                                futures.forEach(f -> f.complete(newValue));
                            }
                        } finally {
                            lock.unlock();
                        }

                    }
                    lastUpdateBarrier.setIfBigger(message.offset());
                });
            }
        }).start();
    }

    private void linearizableRead() {
        String jsonKey = toJson(new Records.WaitKey("read"));

        long offset = totalOrderBroadcast.offer(jsonKey, "true", Records.HeaderValues.WAIT_KEY);
        // wait for the message we sent to get back
        try {
            lastUpdateBarrier.await(offset, 1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String get(Object key) {
        linearizableRead();

        VersionedValue existingVal = data.get(key);
        if (existingVal == null) {
            return null;
        }
        return existingVal.value;
    }

    @Override
    public String put(String key, String value) {
        linearizableRead();
        int nextVersion;
        VersionedValue existingVal = data.get(key);

        if (existingVal == null) {
            nextVersion = 1;
        } else {
            nextVersion = existingVal.version + 1;
        }

        totalOrderBroadcastOffer(key, value, nextVersion);

        if (existingVal == null) {
            return null;
        }
        return existingVal.value;
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends String> m) {
        throw new RuntimeException("Implement me");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Implement me");
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        linearizableRead();
        return data.keySet();
    }

    @NotNull
    @Override
    public Collection<String> values() {
        linearizableRead();
        return data.values().stream().map(versionedValue -> versionedValue.value).toList();
    }

    @NotNull
    @Override
    public Set<Entry<String, String>> entrySet() {
        linearizableRead();
        return data.entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().value))
                .collect(Collectors.toSet());
    }

    public String putIfAbsent(@NotNull String key, @NotNull String value) {
        linearizableRead();
        int nextVersion;
        CompletableFuture<VersionedValue> f = new CompletableFuture<>();


        try {
            lock.lock();
            VersionedValue existingVal = data.get(key);
            if (existingVal != null && existingVal.value != null) {
                return existingVal.value;
            }
            if (existingVal == null) {
                nextVersion = 1;
            } else {
                nextVersion = existingVal.version + 1;
            }

            registerToWaitFirstMessage(f, key, nextVersion);
        } finally {
            lock.unlock();
        }

        totalOrderBroadcastOffer(key, value, nextVersion);

        VersionedValue firstMessageBack = f.join();
        // if the first message we get back is ours. Then putIfAbsent is successful.
        if (value.equals(firstMessageBack.value)) {
            return null;
        }
        // otherwise, there was another set/remove, putIfAbsent failed.
        return firstMessageBack.value;
    }

    private void registerToWaitFirstMessage(CompletableFuture<VersionedValue> f, String key, int version) {
        waitMap.compute(key + ":" + version, (s, futures) -> {
            if (futures == null) {
                futures = new LinkedList<>();
            }
            futures.add(f);
            return futures;
        });
    }

    @Override
    public boolean remove(@NotNull Object key, Object value) {
        throw new RuntimeException("Implement me");
    }

    @Override
    public boolean replace(@NotNull String key, @NotNull String oldValue, @NotNull String newValue) {
        linearizableRead();
        int nextVersion;
        CompletableFuture<VersionedValue> f = new CompletableFuture<>();

        try {
            lock.lock();
            VersionedValue existingVal = data.get(key);
            if (existingVal == null) {
                return false;
            }
            if (!oldValue.equals(existingVal.value)) {
                return false;
            }
            nextVersion = existingVal.version + 1;

            registerToWaitFirstMessage(f, key, nextVersion);
        } finally {
            lock.unlock();
        }

        totalOrderBroadcastOffer(key, newValue, nextVersion);

        VersionedValue firstMessageBack = f.join();
        // if the first message we get back is ours. Then `replace` is successful.
        // otherwise, there was another set/remove, replace failed.
        return newValue.equals(firstMessageBack.value);
    }

    private void totalOrderBroadcastOffer(@NotNull String key, String newValue, int nextVersion) {
        String jsonKey = toJson(new Records.WriteAttemptKey(key));
        String jsonValue = toJson(new Records.WriteAttemptValue(nextVersion, newValue));
        totalOrderBroadcast.offer(jsonKey, jsonValue, Records.HeaderValues.WRITE_ATTEMPT);
    }

    @Override
    public String replace(@NotNull String key, @NotNull String value) {
        linearizableRead();
        int nextVersion;
        VersionedValue existingVal;
        try {
            lock.lock();
            existingVal = data.get(key);
            if (existingVal == null || existingVal.value == null) {
                return null;
            }

            nextVersion = existingVal.version + 1;
        } finally {
            lock.unlock();
        }

        totalOrderBroadcastOffer(key, value, nextVersion);
        return existingVal.value;
    }

    @Override
    public String remove(Object key) {
        linearizableRead();
        int nextVersion;
        VersionedValue existingVal = data.get(key);

        if (existingVal == null) {
            return null;
        } else {
            nextVersion = existingVal.version + 1;
        }

        totalOrderBroadcastOffer((String) key, null, nextVersion);

        return existingVal.value;
    }

    @Override
    public int size() {
        linearizableRead();
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        linearizableRead();
        return data.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        linearizableRead();
        return data.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        linearizableRead();
        return data.containsValue(value);
    }

    @Override
    public void close() {
        isRunning.set(false);
    }
}
