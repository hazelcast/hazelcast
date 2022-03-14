package com.hazelcast.spi.impl.reactor;

import java.util.concurrent.CompletableFuture;

public class Invocation {
    public long callId;
    public Request request;
    public CompletableFuture completableFuture = new CompletableFuture();
}
