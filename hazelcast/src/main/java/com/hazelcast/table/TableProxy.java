package com.hazelcast.table;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.engine.frame.ConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;
import com.hazelcast.spi.impl.requestservice.RequestService;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableService;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.spi.impl.requestservice.OpCodes.GET;
import static com.hazelcast.spi.impl.requestservice.OpCodes.NOOP;
import static com.hazelcast.spi.impl.requestservice.OpCodes.SET;
import static com.hazelcast.spi.impl.requestservice.OpCodes.TABLE_UPSERT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TableProxy<K, V> extends AbstractDistributedObject implements Table<K, V> {

    private final RequestService requestService;
    private final String name;
    private final int partitionCount;
    private final FrameAllocator frameAllocator;
    private final int requestTimeoutMs;

    public TableProxy(NodeEngineImpl nodeEngine, TableService tableService, String name) {
        super(nodeEngine, tableService);
        this.requestService = nodeEngine.getRequestService();
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.frameAllocator = new ConcurrentPooledFrameAllocator(128, true);
        this.requestTimeoutMs = requestService.getRequestTimeoutMs();
    }

    @Override
    public Pipeline newPipeline() {
        return new PipelineImpl(requestService, frameAllocator);
    }

    @Override
    public void upsert(V v) {
        CompletableFuture f = asyncUpsert(v);
        try {
            f.get(23, SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture asyncUpsert(V v) {
        Item item = (Item) v;

        int partitionId = HashUtil.hashToIndex(Long.hashCode(item.key), partitionCount);
        Frame request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, TABLE_UPSERT)
                .writeString(name)
                .writeLong(item.key)
                .writeInt(item.a)
                .writeInt(item.b)
                .writeComplete();
        return requestService.invoke(request, partitionId);
    }

    @Override
    public void concurrentNoop(int concurrency) {
        checkPositive("concurrency", concurrency);

        if (concurrency == 1) {
            try {
                asyncNoop().get(23, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            CompletableFuture[] futures = new CompletableFuture[concurrency];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = asyncNoop();
            }

            for (CompletableFuture f : futures) {
                try {
                    f.get(requestTimeoutMs, MILLISECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void noop() {
        CompletableFuture f = asyncNoop();
        try {
            f.get(requestTimeoutMs, MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture asyncNoop() {
        int partitionId = ThreadLocalRandom.current().nextInt(271);
        Frame request = frameAllocator.allocate(32)
                .newFuture()
                .writeRequestHeader(partitionId, NOOP)
                .writeComplete();
        return requestService.invoke(request, partitionId);
    }

    // better pipelining support
    @Override
    public void upsertAll(V[] values) {
        CompletableFuture[] futures = new CompletableFuture[values.length];
        for (int k = 0; k < futures.length; k++) {
            futures[k] = asyncUpsert(values[k]);
        }

        for (CompletableFuture f : futures) {
            try {
                f.get(requestTimeoutMs, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void set(byte[] key, byte[] value) {
        long hash = HashUtil.MurmurHash3_x64_64(key, 0, key.length);
        int partitionId = HashUtil.hashToIndex(hash, partitionCount);

        Frame request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, SET)
                .writeInt(key.length)
                .writeBytes(key)
                .writeInt(value.length)
                .writeBytes(value)
                .writeComplete();
        CompletableFuture f = requestService.invoke(request, partitionId);
        try {
            f.get(requestTimeoutMs, MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        long hash = HashUtil.MurmurHash3_x64_64(key, 0, key.length);
        int partitionId = HashUtil.hashToIndex(hash, partitionCount);

        Frame request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, GET)
                .writeInt(key.length)
                .writeBytes(key)
                .writeComplete();
        CompletableFuture f = requestService.invoke(request, partitionId);
        try {
            Frame frame = (Frame)f.get(requestTimeoutMs, MILLISECONDS);
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return TableService.SERVICE_NAME;
    }
}
