package com.hazelcast.util;

/**
 * @mdogan 4/30/13
 */

public interface ConstructorFunction<K, V> {

    V createNew(K arg);
}
