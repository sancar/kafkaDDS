package io.github.sancar.kafkadds.linearizable;

import io.github.sancar.kafkadds.totalorderbrodacast.KafkaBroadcast;

import java.util.Properties;

public class Linearizable {

    public static ReplicatedMap newMap(Properties properties) {
        return new ReplicatedMap(new KafkaBroadcast(properties));
    }
}
