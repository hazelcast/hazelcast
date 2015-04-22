/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;

/**
 * Defines managed service behavior of map service.
 *
 * @see MapService
 */
class MapManagedService implements ManagedService {

    private final MapServiceContext mapServiceContext;

    MapManagedService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        mapServiceContext.initPartitionsContainers();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(MapService.SERVICE_NAME,
                    new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                        public LockStoreInfo createNew(final ObjectNamespace key) {
                            final MapContainer mapContainer = mapServiceContext.getMapContainer(key.getObjectName());
                            return new LockStoreInfo() {
                                public int getBackupCount() {
                                    return mapContainer.getBackupCount();
                                }

                                public int getAsyncBackupCount() {
                                    return mapContainer.getAsyncBackupCount();
                                }
                            };
                        }
                    });
        }
        mapServiceContext.getExpirationManager().start();
    }

    @Override
    public void reset() {
        mapServiceContext.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            final MapServiceContext mapServiceContext = this.mapServiceContext;
            mapServiceContext.flushMaps();
            mapServiceContext.destroyMapStores();
            mapServiceContext.clearPartitions();
            mapServiceContext.getNearCacheProvider().clear();
            mapServiceContext.getMapContainers().clear();
        }
    }


}
