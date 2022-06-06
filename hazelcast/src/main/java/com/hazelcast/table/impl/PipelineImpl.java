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

package com.hazelcast.table.impl;

import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.tpc.engine.SyncSocket;
import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import com.hazelcast.tpc.requestservice.RequestService;
import com.hazelcast.table.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.tpc.requestservice.OpCodes.NOOP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


// todo: we don't need a frame for all the requests. We should just add to an existing frame.
public final class PipelineImpl implements Pipeline {

    private final RequestService requestService;
    private final FrameAllocator frameAllocator;
    private final Long2ObjectHashMap longToObjectHashMap = new Long2ObjectHashMap();
    private int partitionId = -1;
    private SyncSocket syncSocket;

    public PipelineImpl(RequestService requestService, FrameAllocator frameAllocator) {
        this.requestService = requestService;
        this.frameAllocator = frameAllocator;
    }

    public void noop(int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("PartitionId can't be smaller than 0");
        }

        if (this.partitionId == -1) {
            this.partitionId = partitionId;
        } else if (partitionId != this.partitionId) {
            throw new RuntimeException("Cross partition request detected; expected " + this.partitionId + " found: " + partitionId);
        }

        Frame request = frameAllocator.allocate(32)
                .writeRequestHeader(partitionId, NOOP)
                .writeComplete();


        //requestService.invokeOnPartition();
    }

    @Override
    public void execute() {
        //requestService.invokeOnPartition(this);
    }

    public void await(){
//        for(Future<Frame> f: futures){
//            try {
//                Frame frame = f.get(requestService.getRequestTimeoutMs(), MILLISECONDS);
//                frame.release();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<Frame> getRequests() {
        return null;//requests;
    }
}
