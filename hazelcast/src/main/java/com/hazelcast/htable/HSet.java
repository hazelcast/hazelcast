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

package com.hazelcast.htable;

import com.hazelcast.core.Command;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.htable.impl.HSetCmd.ID;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HSet implements Command {

    private final TpcRuntime tpcRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;

    public HSet(TpcRuntime tpcRuntime, String name) {
        this.name = name;
        this.tpcRuntime = tpcRuntime;
        this.partitionCount = tpcRuntime.getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
    }

    public void sync(byte[] key, byte[] value) {
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

    public void pipeline(Pipeline pipeline, byte key, byte[] value) {

    }
}
