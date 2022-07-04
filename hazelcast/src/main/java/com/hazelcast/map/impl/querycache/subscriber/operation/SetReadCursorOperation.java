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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulatorOrNull;

/**
 * Sets read cursor of {@code Accumulator} in
 * this partition to the supplied sequence number.
 *
 * @see Accumulator#setHead
 */
public class SetReadCursorOperation
        extends MapOperation implements PartitionAwareOperation {

    private long sequence;
    private String cacheId;

    private transient boolean result;

    public SetReadCursorOperation() {
    }

    public SetReadCursorOperation(String mapName, String cacheId, long sequence, int ignored) {
        super(checkHasText(mapName, "mapName"));
        checkPositive("sequence", sequence);

        this.cacheId = checkHasText(cacheId, "cacheId");
        this.sequence = sequence;
    }

    @Override
    protected void runInternal() {
        this.result = setReadCursor();
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(cacheId);
        out.writeLong(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cacheId = in.readString();
        sequence = in.readLong();
    }

    private boolean setReadCursor() {
        QueryCacheContext context = getContext();
        Accumulator accumulator = getAccumulatorOrNull(context, name, cacheId, getPartitionId());
        if (accumulator == null) {
            return false;
        }
        return accumulator.setHead(sequence);
    }

    private QueryCacheContext getContext() {
        return mapServiceContext.getQueryCacheContext();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.SET_READ_CURSOR;
    }
}
