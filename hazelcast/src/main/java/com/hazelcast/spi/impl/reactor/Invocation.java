package com.hazelcast.spi.impl.reactor;

import java.util.concurrent.CompletableFuture;

public class Invocation {

    public Frame request;
    public CompletableFuture future = new CompletableFuture();
}

