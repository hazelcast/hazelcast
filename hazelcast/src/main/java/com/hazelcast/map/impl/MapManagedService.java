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

package com.hazelcast.map.impl;

import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.locksupport.LockStoreInfo;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.Properties;

/**
 * Defines managed service behavior of map service.
 *
 * @see MapService
 */
public class MapManagedService implements ManagedService {

    private final MapServiceContext mapServiceContext;

    MapManagedService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        final LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(MapService.SERVICE_NAME,
                    new ObjectNamespaceLockStoreInfoConstructorFunction());
        }
        mapServiceContext.initPartitionsContainers();
    }

    @Override
    public void reset() {
        mapServiceContext.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            // in case of a graceful shutdown flush map-stores to prevent data-loss.
            mapServiceContext.flushMaps();
            mapServiceContext.destroyMapStores();
        }

        // clear internal resources, these are the resources wholly managed by hazelcast,
        // means they have no external interaction like map-stores.
        mapServiceContext.shutdown();
    }

    private class ObjectNamespaceLockStoreInfoConstructorFunction implements ConstructorFunction<ObjectNamespace, LockStoreInfo> {
        @Override
        public LockStoreInfo createNew(final ObjectNamespace key) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(key.getObjectName());
            return new LockStoreInfo() {
                @Override
                public int getBackupCount() {
                    return mapContainer.getBackupCount();
                }

                @Override
                public int getAsyncBackupCount() {
                    return mapContainer.getAsyncBackupCount();
                }
            };
        }
    }
}
