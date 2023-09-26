package com.hazelcast.cp;

import com.hazelcast.core.DistributedObject;

public interface CPMap<K, V> extends DistributedObject {

    V get(K key);

    void set(K key, V value);
}
