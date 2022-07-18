package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.iobuffer.IOBuffer;

import java.util.concurrent.CompletableFuture;

public class RequestFuture<T> extends CompletableFuture<T> {

    private IOBuffer request;

    public RequestFuture(IOBuffer request) {
        this.request = request;
    }

    public IOBuffer request() {
        return request;
    }

    public IOBuffer clearRequest() {
        IOBuffer result = request;
        request = null;
        return result;
    }
}
