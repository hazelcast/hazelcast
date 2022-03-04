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

import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.publisher.MapListenerRegistry;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

import java.io.IOException;
import java.util.UUID;

/**
 * This operation removes all {@code QueryCache} resources on a node.
 */
public class DestroyQueryCacheOperation extends AbstractNamedOperation {

    private String cacheId;
    private transient boolean result;

    public DestroyQueryCacheOperation() {
    }

    public DestroyQueryCacheOperation(String mapName, String cacheId) {
        super(mapName);
        this.cacheId = cacheId;
    }

    @Override
    public void run() {
        try {
            deregisterLocalIMapListener();
            removeAccumulatorInfo();
            removePublisherAccumulators();
            removeAllListeners();
            result = true;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Object getResponse() {
        return result;
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

    private void deregisterLocalIMapListener() {
        PublisherContext publisherContext = getPublisherContext();
        MapListenerRegistry registry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry listenerRegistry = registry.getOrNull(name);
        if (listenerRegistry == null) {
            return;
        }
        UUID listenerId = listenerRegistry.remove(cacheId);
        getMapServiceContext().removeEventListener(name, listenerId);
    }

    private void removeAccumulatorInfo() {
        PublisherContext publisherContext = getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.remove(name, cacheId);
    }

    private void removePublisherAccumulators() {
        PublisherContext publisherContext = getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(name);
        if (publisherRegistry == null) {
            return;
        }
        publisherRegistry.remove(cacheId);
    }

    private void removeAllListeners() {
        EventService eventService = getNodeEngine().getEventService();
        eventService.deregisterAllListeners(MapService.SERVICE_NAME, cacheId);
    }

    private PublisherContext getPublisherContext() {
        QueryCacheContext queryCacheContext = getMapServiceContext().getQueryCacheContext();
        return queryCacheContext.getPublisherContext();
    }

    private MapServiceContext getMapServiceContext() {
        MapService service = getService();
        return service.getMapServiceContext();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.DESTROY_QUERY_CACHE;
    }
}
