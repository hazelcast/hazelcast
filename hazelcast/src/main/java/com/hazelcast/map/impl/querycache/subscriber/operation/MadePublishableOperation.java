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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.utils.QueryCacheUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

import java.io.IOException;

/**
 * Sets {@link AccumulatorInfo#publishable} to {@code true}.
 *
 * After enabling that, accumulators becomes available
 * to send events in their buffers to subscriber-side.
 */
public class MadePublishableOperation extends AbstractNamedOperation {

    private final ILogger logger = Logger.getLogger(getClass());

    private String cacheId;

    private transient boolean done;

    public MadePublishableOperation() {
    }

    public MadePublishableOperation(String mapName, String cacheId) {
        super(mapName);
        this.cacheId = cacheId;
    }

    @Override
    public void run() {
        setPublishable();
    }

    private void setPublishable() {
        PartitionAccumulatorRegistry registry
                = QueryCacheUtil.getAccumulatorRegistryOrNull(getContext(), name, cacheId);
        if (registry == null) {
            return;
        }

        AccumulatorInfo info = registry.getInfo();
        info.setPublishable(true);

        if (logger.isFinestEnabled()) {
            logger.finest("Accumulator was made publishable for map=" + getName());
        }
        this.done = true;
    }

    private QueryCacheContext getContext() {
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }


    @Override
    public Object getResponse() {
        return done;
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

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MADE_PUBLISHABLE;
    }
}
