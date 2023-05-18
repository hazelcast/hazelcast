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

package com.hazelcast.internal.tpc;

import com.hazelcast.htable.Pipeline;
import com.hazelcast.htable.impl.HGetCmd;
import com.hazelcast.htable.impl.HSetCmd;
import com.hazelcast.internal.tpc.server.PipelineCmd;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.iobuffer.IOBufferAllocator;
import com.hazelcast.noop.impl.NopCmd;

import java.util.Arrays;

import static com.hazelcast.internal.util.HashUtil.hashToIndex;

public final class PipelineImpl implements Pipeline {

    private final TpcRuntime tpcRuntime;
    private final IOBufferAllocator requestAllocator;
    private final int partitionCount;
    private int partitionId = -1;
    public IOBuffer request;
    private int countPos;
    public int count;

    public PipelineImpl(TpcRuntime tpcRuntime, IOBufferAllocator requestAllocator) {
        this.tpcRuntime = tpcRuntime;
        this.requestAllocator = requestAllocator;
        this.partitionCount = tpcRuntime.getPartitionCount();
        this.request = new IOBuffer(64 * 1024);//requestAllocator.allocate();
    }

    public void noop(int partitionId) {
        init(partitionId);

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // commandid
        request.writeInt(NopCmd.ID);
        // set the size.
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    @Override
    public void get(byte[] key) {
        init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // commandid
        request.writeInt(HGetCmd.ID);
        // writing the key
        request.writeSizePrefixedBytes(key);
        // fixing the size
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        init(hashToIndex(Arrays.hashCode(key), partitionCount));

        int sizePos = request.position();
        // size placeholder
        request.writeInt(0);
        // commandid
        request.writeInt(HSetCmd.ID);
        // writing the key
        request.writeSizePrefixedBytes(key);
        // writing the key
        request.writeSizePrefixedBytes(value);
        // fixing the size
        request.putInt(sizePos, request.position() - sizePos);

        count++;
    }

    public void init(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0");
        }

        if (partitionId > partitionCount - 1) {
            throw new IllegalArgumentException("PartitionId can't be larger than " + (partitionCount - 1) + " but was:" + partitionId);
        }

        if (this.partitionId == -1) {
            this.partitionId = partitionId;
            FrameCodec.writeRequestHeader(request, partitionId, PipelineCmd.ID);
            countPos = request.position();
            request.writeInt(0);
        } else if (partitionId != this.partitionId) {
            throw new RuntimeException("Cross partition request detected; expected "
                    + this.partitionId + " found: " + partitionId);
        }
    }

    @Override
    public void execute() {
        request.putInt(countPos, count);
        FrameCodec.setSize(request);

        RequestFuture<IOBuffer> requestFuture = tpcRuntime.getRpcCore().invoke(partitionId, request);
        IOBuffer response = requestFuture.join();
        response.release();
    }

    @Override
    public void reset() {
        partitionId = -1;
        //this.request = requestAllocator.allocate();
        request.clear();
        count = 0;
    }
}
