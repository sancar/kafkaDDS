package io.github.sancar.kafkadds.totalorderbrodacast;


import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;

public class KafkaBroadcast implements TotalOrderBroadcast {

    private static final String topic = "linearizablemap";
    private static final UUID instanceId = UUID.randomUUID();

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    public KafkaBroadcast(Properties userProperties) {
        createTopic(userProperties);
        consumer = new KafkaConsumer<>(consumerProperties(userProperties));
        consumer.subscribe(Collections.singleton(topic));
        producer = new KafkaProducer<>(producerProperties(userProperties));
    }

    private static void createTopic(Properties userProperties) {
        try (Admin admin = Admin.create(userProperties)) {
            try {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 3);
                HashMap<String, String> map = new HashMap<>();
                map.put("min.compaction.lag.ms", String.valueOf(TimeUnit.HOURS.toMillis(1)));
                map.put("cleanup.policy", "compact");
                newTopic.configs(map);
                admin.createTopics(singleton(newTopic))
                        .all().get();
            } catch (ExecutionException executionException) {
                if (executionException.getCause() instanceof TopicExistsException) {
                    DescribeTopicsResult describeTopicsResult = admin.describeTopics(singleton(topic));
                    try {
                        var topicIdValues = describeTopicsResult.topicNameValues();
                        var desc = topicIdValues.get(topic).get();
                        int partitionCount = desc.partitions().size();
                        if (partitionCount != 1) {
                            throw new RuntimeException("Partition count of " + topic + " must be 1");
                        }
                        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                        var configs = admin.describeConfigs(singleton(configResource)).values();
                        Config config = configs.get(configResource).get();
                        ConfigEntry configEntry = config.get("min.compaction.lag.ms");
                        if (Long.parseLong(configEntry.value()) < TimeUnit.HOURS.toMillis(1)) {
                            throw new RuntimeException("`min.compaction.lag.ms` is too short for replicated map. Make it at least 1 hour");
                        }
                        ConfigEntry cleanupPolicy = config.get("cleanup.policy");
                        if (!cleanupPolicy.value().equals("compact")) {
                            throw new RuntimeException("`cleanup.policy` must be `compact` for replicated map.");
                        }

                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new RuntimeException(executionException.getCause());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Properties producerProperties(Properties props) {
        var name = "linearizableProducer/" + instanceId;
        props.put(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, ClientDnsLookup.USE_ALL_DNS_IPS.toString());
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, name);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        return props;
    }

    private Properties consumerProperties(Properties props) {
        props.put(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, ClientDnsLookup.USE_ALL_DNS_IPS.toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "-" + UUID.randomUUID());
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "linearizableConsumer" + "-" + instanceId);
        return props;
    }

    private record MessageHeader(String key, String v) implements Header {

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return v.getBytes();
        }
    }

    @Override
    public long offer(String key, String value, String header) {
        List<Header> h = Collections.singletonList(new MessageHeader(Records.HEADER_KEY_OPERATION, header));
        var future = producer.send(new ProducerRecord<String, String>(topic, 0, key, value, h));
        try {
            RecordMetadata metadata = future.get();
            return metadata.offset();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<Message> consume() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        ArrayList<Message> rs = new ArrayList<>();
        records.forEach(r -> {
            String op = new String(r.headers().lastHeader(Records.HEADER_KEY_OPERATION).value());
            rs.add(new Message(r.offset(), r.key(), r.value(), op));
        });
        return rs;
    }
}
