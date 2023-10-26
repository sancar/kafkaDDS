# Distributed Data Structures On Kafka

This is an experimental repo to study Distributed Data Structures and Kafka.

## Linearizable Replicated Map 

See related blog for design:
https://upstash.com/blog/linearizable-dist-map-on-kafka


Get your free kafka here to try: https://upstash.com/

Usage: 

```java

Properties props = new Properties();
props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
props.setProperty("security.protocol", "SASL_SSL");
props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"UPSTASH_USERNAME\" password=\"UPSTAH_PASSWORD\";");
props.setProperty("bootstrap.servers", "UPSTASH_ENDPOINT");

ConcurrentMap<String,String> map = Linearizable.newMap(props);
map.put("hello", "world");

```


