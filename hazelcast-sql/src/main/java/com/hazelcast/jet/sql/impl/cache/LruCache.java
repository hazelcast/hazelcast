package com.hazelcast.jet.sql.impl.cache;

import java.io.Serializable;
import java.util.function.Function;

public interface LruCache<K, V> extends Serializable {
    V computeIfAbsent(K key, Function<K, V> valueFunction);
}
