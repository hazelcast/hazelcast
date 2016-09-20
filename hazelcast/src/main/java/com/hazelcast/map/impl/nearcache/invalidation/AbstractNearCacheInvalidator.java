/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import static com.hazelcast.core.EntryEventType.INVALIDATION;


/**
 * Contains common functionality of a {@code NearCacheInvalidator}
 */
public abstract class AbstractNearCacheInvalidator implements NearCacheInvalidator {

    protected final EventService eventService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final MapServiceContext mapServiceContext;
    protected final NearCacheProvider nearCacheProvider;
    protected final NodeEngine nodeEngine;

    public AbstractNearCacheInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        this.mapServiceContext = mapServiceContext;
        this.nearCacheProvider = nearCacheProvider;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = nodeEngine.getEventService();
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
    }

    protected final Data toHeapData(Data key) {
        if (key == null) {
            return null;
        }

        return mapServiceContext.toData(key);
    }

    protected static int orderKey(SingleNearCacheInvalidation invalidation) {
        Data key = invalidation.getKey();

        if (key != null) {
            return key.hashCode();
        }

        return invalidation.getName().hashCode();
    }

    protected final boolean canSendInvalidationEvent(final EventRegistration registration, final String sourceUuid) {
        EventFilter filter = registration.getFilter();

        if (!(filter instanceof EventListenerFilter)) {
            return false;
        }

        if (!filter.eval(INVALIDATION.getType())) {
            return false;
        }

        EventFilter unwrappedEventFilter = ((EventListenerFilter) filter).getEventFilter();
        if (unwrappedEventFilter.eval(sourceUuid)) {
            return false;
        }

        return true;
    }

    @Override
    public void destroy(String mapName, String sourceUuid) {
        // nop.
    }

    @Override
    public void reset() {
        // nop.
    }

    @Override
    public void shutdown() {
        // nop.
    }

}
