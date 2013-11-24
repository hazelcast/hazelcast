package com.hazelcast.core;

import java.io.Serializable;

public interface Function<T,R> extends Serializable {

    R apply(T input);
}
