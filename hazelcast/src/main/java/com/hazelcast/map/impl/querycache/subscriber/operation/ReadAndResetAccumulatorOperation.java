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

package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulators;

/**
 * Reads all available items from the accumulator of the partition
 * and resets it. This operation is used to retrieve the events
 * which are buffered during the initial snapshot taking phase.
 *
 * @see com.hazelcast.map.impl.querycache.subscriber.operation.PublisherCreateOperation
 */
public class ReadAndResetAccumulatorOperation
        extends MapOperation implements PartitionAwareOperation {

    private String cacheId;
    private List<Sequenced> eventDataList;

    public ReadAndResetAccumulatorOperation() {
    }

    public ReadAndResetAccumulatorOperation(String mapName, String cacheId) {
        super(mapName);
        this.cacheId = cacheId;
    }

    @Override
    protected void runInternal() {
        QueryCacheContext context = getQueryCacheContext();
        Map<Integer, Accumulator> accumulators = getAccumulators(context, name, cacheId);
        Accumulator<Sequenced> accumulator = accumulators.get(getPartitionId());
        if (accumulator == null || accumulator.isEmpty()) {
            return;
        }

        eventDataList = new ArrayList<>(accumulator.size());
        for (Sequenced sequenced : accumulator) {
            eventDataList.add(sequenced);
        }

        accumulator.reset();
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return eventDataList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(cacheId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cacheId = in.readString();
    }

    private QueryCacheContext getQueryCacheContext() {
        return mapServiceContext.getQueryCacheContext();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.READ_AND_RESET_ACCUMULATOR;
    }
}
