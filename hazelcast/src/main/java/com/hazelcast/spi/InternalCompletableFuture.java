package com.hazelcast.spi;

import com.hazelcast.core.CompletableFuture;

public interface InternalCompletableFuture<E> extends CompletableFuture<E>{
    E getSafely();
}
