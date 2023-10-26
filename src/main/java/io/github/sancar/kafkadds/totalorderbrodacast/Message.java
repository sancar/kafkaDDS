package io.github.sancar.kafkadds.totalorderbrodacast;

public record Message(long offset, String key, String value, String op) {
}

