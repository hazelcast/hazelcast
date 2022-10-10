package com.hazelcast.bulktransport.impl;

import com.hazelcast.bulktransport.BulkTransport;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.alto.AltoRuntime;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkTransportImpl implements BulkTransport {

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AltoRuntime altoRuntime;
    private final Address address;
    private final int reactor;
    private final int receiveBufferSize;
    private AsyncSocket[] channels;
    private IOBufferAllocator frameAllocator;

    public BulkTransportImpl(AltoRuntime altoRuntime, Address address, int reactor) {
        this.altoRuntime = altoRuntime;
        this.address = address;
        this.reactor = reactor;
        this.receiveBufferSize = 0;
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("BulkTransport is closed");
        }
    }

    public void connect() {
//        CompletableFuture[] futures = new CompletableFuture[channels.length];
//        for (int k = 0; k < futures.length; k++) {
//            futures[k] = altoRuntime.connect(null, 0);
//        }

        //CompletableFuture.allOf(futures);
    }

    @Override
    public void copyFile(File file) {
        ensureOpen();
    }

    @Override
    public void copyMemory(long address, long length) {
        ensureOpen();
    }

    @Override
    public void copyFake(long length) {
        ensureOpen();

//        CompletableFuture[] futures = new CompletableFuture[channels.length];
//        for (AsyncSocket channel : channels) {
//            IOBuffer request = frameAllocator.allocate()
//                    .writeRequestHeader(-1, INIT_BULK_TRANSPORT)
//                    .constructComplete();
//
//            altoRuntime.invoke(request, channel).thenAccept(o -> {
//                IOBuffer request1 = frameAllocator.allocate()
//                        .writeRequestHeader(-1, BULK_TRANSPORT)
//                        .constructComplete();
//                CompletableFuture future1 = altoRuntime.invoke(request1, channel);
//            });
//        }

       // allOf(futures);

    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            for (AsyncSocket channel : channels) {
                channel.close();
            }
            channels = null;
        }
    }
}
