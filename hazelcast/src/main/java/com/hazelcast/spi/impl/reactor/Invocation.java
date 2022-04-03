package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;

import java.util.concurrent.CompletableFuture;

public class Invocation {

    public Address target;
    public Frame request;
    public CompletableFuture future = new CompletableFuture();
}

