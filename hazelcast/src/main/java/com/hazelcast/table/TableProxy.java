/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.table;

import com.hazelcast.bulktransport.BulkTransport;
import com.hazelcast.bulktransport.impl.BulkTransportImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.alto.AltoRuntime;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.PartitionActorRef;
import com.hazelcast.internal.tpc.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.pubsub.Publisher;
import com.hazelcast.pubsub.Subscriber;
import com.hazelcast.pubsub.impl.PublisherImpl;
import com.hazelcast.pubsub.impl.SubscriberImpl;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.alto.OpCodes.GET;
import static com.hazelcast.internal.alto.OpCodes.NOOP;
import static com.hazelcast.internal.alto.OpCodes.QUERY;
import static com.hazelcast.internal.alto.OpCodes.SET;
import static com.hazelcast.internal.alto.OpCodes.TABLE_UPSERT;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TableProxy<K, V> extends AbstractDistributedObject implements Table<K, V> {

    private final AltoRuntime altoRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;
    private final PartitionActorRef[] partitionActorRefs;

    public TableProxy(NodeEngineImpl nodeEngine, TableService tableService, String name) {
        super(nodeEngine, tableService);
        this.altoRuntime = nodeEngine.getNode().getAltoRuntime();
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.requestTimeoutMs = altoRuntime.getRequestTimeoutMs();
        this.partitionActorRefs = altoRuntime.partitionActorRefs();
    }

    @Override
    public Pipeline newPipeline() {
        return new PipelineImpl(altoRuntime, requestAllocator);
    }

    @Override
    public Publisher createPublisher(String topic) {
        return new PublisherImpl(topic, partitionActorRefs);
    }

    @Override
    public Subscriber createSubsriber(String topic) {
        return new SubscriberImpl(topic, partitionActorRefs);
    }

    @Override
    public void upsert(V v) {
        CompletableFuture<IOBuffer> f = asyncUpsert(v);
        try {
            IOBuffer response = f.get(23, SECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture asyncUpsert(V v) {
        Item item = (Item) v;

        int partitionId = hashToIndex(Long.hashCode(item.key), partitionCount);
        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, TABLE_UPSERT);
        request.writeString(name);
        request.writeLong(item.key);
        request.writeInt(item.a);
        request.writeInt(item.b);
        FrameCodec.setSize(request);
        return partitionActorRefs[partitionId].submit(request);
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
    public void concurrentNoop(int concurrency) {
        int partitionId = ThreadLocalRandom.current().nextInt(partitionCount);
        concurrentNoop(concurrency, partitionId);
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

    @Override
    public void noop() {
        int partitionId = ThreadLocalRandom.current().nextInt(partitionCount);

        CompletableFuture<IOBuffer> f = asyncNoop(partitionId);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<IOBuffer> asyncNoop() {
        return asyncNoop(ThreadLocalRandom.current().nextInt(partitionCount));
    }

    private CompletableFuture<IOBuffer> asyncNoop(int partitionId) {
      //  ConcurrentIOBufferAllocator allocator = new ConcurrentIOBufferAllocator(1,true);
        IOBuffer request = requestAllocator.allocate(32);
     //   request.trackRelease=true;
        FrameCodec.writeRequestHeader(request, partitionId, NOOP);
        FrameCodec.setSize(request);
        return partitionActorRefs[partitionId].submit(request);
    }

    // better pipelining support
    @Override
    public void upsertAll(V[] values) {
        CompletableFuture[] futures = new CompletableFuture[values.length];
        for (int k = 0; k < futures.length; k++) {
            futures[k] = asyncUpsert(values[k]);
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

    @Override
    public BulkTransport newBulkTransport(Address address, int parallelism) {
        return new BulkTransportImpl(altoRuntime, address, parallelism);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);

        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, SET);
        request.writeSizedBytes(key);
        request.writeSizedBytes(value);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = partitionActorRefs[partitionId].submit(request);
        try {
            IOBuffer response = f.get(requestTimeoutMs, MILLISECONDS);
            response.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void bogusQuery() {
        CompletableFuture[] futures = new CompletableFuture[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            IOBuffer request = requestAllocator.allocate(60);
            FrameCodec.writeRequestHeader(request, partitionId, QUERY);
            FrameCodec.setSize(request);
            futures[partitionId] = partitionActorRefs[partitionId].submit(request);
        }

        for (CompletableFuture future : futures) {
            try {
                future.get(requestTimeoutMs, MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public byte[] get(byte[] key) {
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);
        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, GET);
        request.writeSizedBytes(key);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = partitionActorRefs[partitionId].submit(request);
        IOBuffer response = null;
        try {
            response = f.get(requestTimeoutMs, MILLISECONDS);
            int length = response.readInt();

            if (length == -1) {
                return null;
            } else {
                byte[] bytes = new byte[length];
                response.readBytes(bytes, length);
                return bytes;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (response != null) {
                response.release();
            }
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
