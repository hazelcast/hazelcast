/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.htable.impl;

import com.hazelcast.htable.HTable;
import com.hazelcast.htable.Pipeline;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.server.ServerTpcRuntime;
import com.hazelcast.internal.tpc.PipelineImpl;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.htable.impl.HSetCmd.ID;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HTableProxy extends AbstractDistributedObject implements HTable {

    private final ServerTpcRuntime tpcRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;

    public HTableProxy(NodeEngineImpl nodeEngine, HTableService tableService, String name) {
        super(nodeEngine, tableService);
        this.name = name;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.tpcRuntime = nodeEngine.getNode().getTpcRuntime();
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
    }

    @Override
    public Pipeline newPipeline() {
        return new PipelineImpl(tpcRuntime, requestAllocator);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        int partitionId = hashToIndex(Arrays.hashCode(key), partitionCount);

        IOBuffer request = requestAllocator.allocate(60);
        FrameCodec.writeRequestHeader(request, partitionId, ID);
        request.writeSizePrefixedBytes(key);
        request.writeSizePrefixedBytes(value);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = tpcRuntime.getRpcCore().invoke(partitionId, request);
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
            FrameCodec.writeRequestHeader(request, partitionId, QueryCmd.ID);
            FrameCodec.setSize(request);
            futures[partitionId] = tpcRuntime.getRpcCore().invoke(partitionId, request);
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
        FrameCodec.writeRequestHeader(request, partitionId, ID);
        request.writeSizePrefixedBytes(key);
        FrameCodec.setSize(request);
        CompletableFuture<IOBuffer> f = tpcRuntime.getRpcCore().invoke(partitionId, request);
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
        return HTableService.SERVICE_NAME;
    }
}
