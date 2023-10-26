package io.github.sancar.kafkadds.totalorderbrodacast;

import java.util.Collection;

public interface TotalOrderBroadcast {

    long offer(String key, String value, String header);

    Collection<Message> consume();
}
