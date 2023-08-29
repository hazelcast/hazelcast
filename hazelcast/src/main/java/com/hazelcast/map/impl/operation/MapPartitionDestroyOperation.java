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

package com.hazelcast.map.impl.operation;


import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.tenantcontrol.TenantControl;

/**
 * Operation to destroy the map data on the partition thread
 */
public class MapPartitionDestroyOperation extends AbstractMapLocalOperation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private final PartitionContainer partitionContainer;
    private final MapContainer mapContainer;

    public MapPartitionDestroyOperation(PartitionContainer partitionContainer,
                                        MapContainer mapContainer) {
        super(mapContainer.getName());
        this.createRecordStoreOnDemand = false;
        this.partitionContainer = partitionContainer;
        this.mapContainer = mapContainer;
    }

    @Override
    protected void runInternal() {
        partitionContainer.destroyMap(mapContainer);
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public TenantControl getTenantControl() {
        return TenantControl.NOOP_TENANT_CONTROL;
    }
}
