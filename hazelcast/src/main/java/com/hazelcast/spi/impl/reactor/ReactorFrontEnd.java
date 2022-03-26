package com.hazelcast.spi.impl.reactor;

import java.util.concurrent.CompletableFuture;

public interface ReactorFrontEnd {

    CompletableFuture invoke(Request request);

    void start();

    void shutdown();
}
