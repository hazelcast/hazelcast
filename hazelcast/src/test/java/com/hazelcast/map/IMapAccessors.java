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

package com.hazelcast.map;

import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.recordstore.RecordStore;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility accessors for IMap internals.
 * <p>
 * Works only with server side maps.
 */
public final class IMapAccessors {

    private IMapAccessors() {
    }

    /**
     * @return existing map-container or {@code null}.
     */
    @Nullable
    public static MapContainer getExistingMapContainer(IMap map) {
        assertMapImpl(map);

        MapServiceContext mapServiceContext = getMapServiceContext(map);
        return mapServiceContext.getMapContainers().get(map.getName());
    }

    @Nonnull
    public static MapServiceContext getMapServiceContext(IMap map) {
        assertMapImpl(map);

        MapProxyImpl mapProxy = (MapProxyImpl) map;
        RemoteService service = mapProxy.getService();
        MapService mapService = (MapService) service;
        return mapService.getMapServiceContext();
    }

    @Nonnull
    public static List<RecordStore> getAllRecordStoresOf(IMap map) {
        assertMapImpl(map);

        List<RecordStore> recordStores = new ArrayList<>();

        MapServiceContext mapServiceContext = getMapServiceContext(map);
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(i);
            RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(map.getName());
            if (existingRecordStore != null) {
                recordStores.add(existingRecordStore);
            }
        }
        return recordStores;
    }


    public static int getPendingOffloadedOpCount(IMap map) {
        int count = 0;
        List<RecordStore> recordStores = IMapAccessors.getAllRecordStoresOf(map);
        for (RecordStore recordStore : recordStores) {
            count += recordStore.getOffloadedOperations().size();
        }
        return count;
    }

    private static void assertMapImpl(IMap map) {
        assert map instanceof MapProxyImpl
                : "Works only with server side map";
    }
}
