package com.hazelcast.internal.query.io;

public interface Row {
    Object get(int idx);
    int columnCount();
}
