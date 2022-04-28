package com.hazelcast.bulktransport.impl;

import com.hazelcast.bulktransport.BulkTransport;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.engine.AsyncSocket;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;
import com.hazelcast.spi.impl.requestservice.RequestService;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.impl.requestservice.OpCodes.BULK_TRANSPORT;
import static com.hazelcast.spi.impl.requestservice.OpCodes.INIT_BULK_TRANSPORT;
import static java.util.concurrent.CompletableFuture.allOf;

public class BulkTransportImpl implements BulkTransport {

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final RequestService requestService;
    private final Address address;
    private final int reactor;
    private final int receiveBufferSize;
    private AsyncSocket[] channels;
    private FrameAllocator frameAllocator;

    public BulkTransportImpl(RequestService requestService, Address address, int reactor) {
        this.requestService = requestService;
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
//            futures[k] = requestService.connect(null, 0);
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

        CompletableFuture[] futures = new CompletableFuture[channels.length];
        for (AsyncSocket channel : channels) {
            Frame request = frameAllocator.allocate()
                    .newFuture()
                    .writeRequestHeader(-1, INIT_BULK_TRANSPORT)
                    .writeComplete();

            requestService.invoke(request, channel).thenAccept(o -> {
                Frame request1 = frameAllocator.allocate()
                        .newFuture()
                        .writeRequestHeader(-1, BULK_TRANSPORT)
                        .writeComplete();
                CompletableFuture future1 = requestService.invoke(request1, channel);
            });
        }

        allOf(futures);

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
