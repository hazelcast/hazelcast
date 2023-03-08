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
import com.hazelcast.htable.impl.HGetCmd;
import com.hazelcast.internal.tpc.PipelineImpl;
import com.hazelcast.internal.tpc.FrameCodec;
import com.hazelcast.internal.tpc.TpcRuntime;
import com.hazelcast.internal.tpcengine.iobuffer.ConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.htable.impl.HGetCmd.ID;
import static com.hazelcast.internal.util.HashUtil.hashToIndex;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HGet implements Command {
    private final TpcRuntime tpcRuntime;
    private final String name;
    private final int partitionCount;
    private final IOBufferAllocator requestAllocator;
    private final int requestTimeoutMs;

    public HGet(TpcRuntime tpcRuntime, String name) {
        this.name = name;
        this.tpcRuntime = tpcRuntime;
        this.partitionCount = tpcRuntime.getPartitionCount();
        this.requestAllocator = new ConcurrentIOBufferAllocator(128, true);
        this.requestTimeoutMs = tpcRuntime.getRequestTimeoutMs();
    }

    public byte[] sync(byte[] key) {
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

    public void pipeline(Pipeline p, byte[] key) {
        PipelineImpl pipeline = (PipelineImpl) p;
        pipeline.init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = pipeline.request.position();
        // size placeholder
        pipeline.request.writeInt(0);
        // opcode
        pipeline.request.writeInt(HGetCmd.ID);
        // writing the key
        pipeline.request.writeSizePrefixedBytes(key);
        // fixing the size
        pipeline.request.putInt(sizePos, pipeline.request.position() - sizePos);

        pipeline.count++;
    }
}
