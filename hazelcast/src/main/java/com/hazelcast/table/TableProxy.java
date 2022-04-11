package com.hazelcast.table;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.reactor.ConcurrentPooledFrameAllocator;
import com.hazelcast.spi.impl.reactor.Frame;
import com.hazelcast.spi.impl.reactor.FrameAllocator;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableService;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_NOOP;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TableProxy<K, V> extends AbstractDistributedObject implements Table<K, V> {

    private final ReactorFrontEnd frontEnd;
    private final String name;
    private final int partitionCount;
    private final FrameAllocator frameAllocator;

    public TableProxy(NodeEngineImpl nodeEngine, TableService tableService, String name) {
        super(nodeEngine, tableService);
        this.frontEnd = nodeEngine.getReactorFrontEnd();
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.frameAllocator = new ConcurrentPooledFrameAllocator(128, true);
    }

    @Override
    public Pipeline newPipeline() {
        return new PipelineImpl(frontEnd, frameAllocator);
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
                .completeWriting();
        return frontEnd.invoke(request, partitionId);
    }

    @Override
    public void concurrentNoop(int concurrency) {
        checkPositive("concurrency", concurrency);

        if (concurrency == 1) {
            try {
                asyncNoop().get(23, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        } else {
            CompletableFuture[] futures = new CompletableFuture[concurrency];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = asyncNoop();
            }

            for (CompletableFuture f : futures) {
                try {
                    f.get(23, SECONDS);
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
            f.get(23, SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture asyncNoop() {
        int partitionId = ThreadLocalRandom.current().nextInt(271);
        Frame request = frameAllocator.allocate(32)
                .newFuture()
                .writeRequestHeader(partitionId, TABLE_NOOP)
                .completeWriting();
        return frontEnd.invoke(request, partitionId);
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
                f.get(23, SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void selectByKey(K key) {
        throw new RuntimeException();
    }

    @Override
    public String getName() {
        return name;
    }

    @NotNull
    @Override
    public DestroyEventContext getDestroyContextForTenant() {
        return super.getDestroyContextForTenant();
    }

    @Override
    public String getServiceName() {
        return TableService.SERVICE_NAME;
    }

}
