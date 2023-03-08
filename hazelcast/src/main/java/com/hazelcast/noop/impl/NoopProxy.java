package com.hazelcast.noop.impl;

import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.noop.Noop;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.noop.impl.NopCmd.ID;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NoopProxy extends AbstractDistributedObject implements Noop {

    private final ConcurrentIOBufferAllocator requestAllocator;
    private final TpcRuntime tpcRuntime;
    private final int requestTimeoutMs;
    private final String name;
    private final int partitionCount;

    public NoopProxy(NodeEngineImpl nodeEngine, NoopService nopService, String name) {
        super(nodeEngine, nopService);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return NoopService.SERVICE_NAME;
    }

    @Override
    public void concurrentNoop(int concurrency, int partitionId) {
        checkPositive("concurrency", concurrency);

        if (concurrency == 1) {
            try {
                IOBuffer response = asyncNoop(partitionId).get(23, SECONDS);
                response.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            CompletableFuture[] futures = new CompletableFuture[concurrency];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = asyncNoop(partitionId);
            }

            for (CompletableFuture<IOBuffer> f : futures) {
                try {
                    IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
                    response.release();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void noop(int partitionId) {
        CompletableFuture<IOBuffer> f = asyncNoop(partitionId);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<IOBuffer> asyncNoop(int partitionId) {
        //  ConcurrentIOBufferAllocator allocator = new ConcurrentIOBufferAllocator(1,true);
        IOBuffer request = requestAllocator.allocate(32);
        //   request.trackRelease=true;
        FrameCodec.writeRequestHeader(request, partitionId, ID);
        FrameCodec.setSize(request);
        return tpcRuntime.getRpcCore().invoke(partitionId, request);
    }

}
