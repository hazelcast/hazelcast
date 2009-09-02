package com.hazelcast.query;

public interface IndexedPredicate {
    String getIndexName();

    Object getValue();
}
