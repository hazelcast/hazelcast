package com.hazelcast.jet2;

import java.io.Serializable;

@FunctionalInterface
public interface KeyExtractor<E, K> extends Serializable {
    K extract(E item);
}
