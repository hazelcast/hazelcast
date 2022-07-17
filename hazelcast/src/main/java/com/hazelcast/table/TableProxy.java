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
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.tpc.engine.iobuffer.ParallelIOBufferAllocator;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import com.hazelcast.tpc.engine.iobuffer.IOBufferAllocator;
import com.hazelcast.tpc.requestservice.PartitionActorRef;
import com.hazelcast.tpc.requestservice.RequestService;
import com.hazelcast.bulktransport.impl.BulkTransportImpl;
import com.hazelcast.table.impl.PipelineImpl;
import com.hazelcast.table.impl.TableService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.tpc.requestservice.OpCodes.GET;
import static com.hazelcast.tpc.requestservice.OpCodes.NOOP;
import static com.hazelcast.tpc.requestservice.OpCodes.QUERY;
import static com.hazelcast.tpc.requestservice.OpCodes.SET;
import static com.hazelcast.tpc.requestservice.OpCodes.TABLE_UPSERT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TableProxy<K, V> extends AbstractDistributedObject implements Table<K, V> {

    private final RequestService requestService;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator frameAllocator;
    private final int requestTimeoutMs;
    private final PartitionActorRef[] partitionActorRefs;

    public TableProxy(NodeEngineImpl nodeEngine, TableService tableService, String name) {
        super(nodeEngine, tableService);
        this.requestService = nodeEngine.getRequestService();
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.frameAllocator = new ParallelIOBufferAllocator(128, true);
        this.requestTimeoutMs = requestService.getRequestTimeoutMs();
        this.partitionActorRefs = requestService.partitionActorRefs();
    }

    @Override
    public Pipeline newPipeline() {
        return new PipelineImpl(requestService, frameAllocator);
    }

    @Override
    public void upsert(V v) {
        CompletableFuture<IOBuffer> f = asyncUpsert(v);
        try {
            IOBuffer frame = f.get(23, SECONDS);
            frame.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture asyncUpsert(V v) {
        Item item = (Item) v;

        int partitionId = hashToIndex(Long.hashCode(item.key), partitionCount);
        IOBuffer request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, TABLE_UPSERT)
                .writeString(name)
                .writeLong(item.key)
                .writeInt(item.a)
                .writeInt(item.b)
                .constructComplete();
        return partitionActorRefs[partitionId].submit(request);
    }

    @Override
    public void concurrentNoop(int concurrency) {
        checkPositive("concurrency", concurrency);

        if (concurrency == 1) {
            try {
                IOBuffer frame = asyncNoop().get(23, SECONDS);
                frame.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            CompletableFuture[] futures = new CompletableFuture[concurrency];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = asyncNoop();
            }

            for (CompletableFuture<IOBuffer> f : futures) {
                try {
                    IOBuffer frame = f.get(requestTimeoutMs, MILLISECONDS);
                    frame.release();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void noop() {
        CompletableFuture<IOBuffer> f = asyncNoop();
        try {
            IOBuffer frame = f.get(requestTimeoutMs, MILLISECONDS);
            frame.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<IOBuffer> asyncNoop() {
        int partitionId = ThreadLocalRandom.current().nextInt(partitionCount);
        IOBuffer request = frameAllocator.allocate(32)
                .newFuture()
                .writeRequestHeader(partitionId, NOOP)
                .constructComplete();
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
                IOBuffer frame = f.get(requestTimeoutMs, MILLISECONDS);
                frame.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public BulkTransport newBulkTransport(Address address, int parallelism) {
        return new BulkTransportImpl(requestService, address, parallelism);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);

        IOBuffer request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, SET)
                .writeSizedBytes(key)
                .writeSizedBytes(value)
                .constructComplete();
        CompletableFuture<IOBuffer> f = partitionActorRefs[partitionId].submit(request);
        try {
            IOBuffer frame = f.get(requestTimeoutMs, MILLISECONDS);
            frame.release();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void bogusQuery() {
        CompletableFuture[] futures = new CompletableFuture[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            IOBuffer request = frameAllocator.allocate(60)
                    .newFuture()
                    .writeRequestHeader(partitionId, QUERY)
                    .constructComplete();
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

        IOBuffer request = frameAllocator.allocate(60)
                .newFuture()
                .writeRequestHeader(partitionId, GET)
                .writeSizedBytes(key)
                .constructComplete();
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
