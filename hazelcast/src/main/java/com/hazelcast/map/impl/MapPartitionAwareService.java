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

package com.hazelcast.map.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;

import java.util.LinkedList;

/**
 * Defines partition-aware operations' behavior of map service.
 * Currently, it only defines the behavior for partition lost occurrences
 *
 * @see com.hazelcast.partition.InternalPartitionLostEvent
 */
class MapPartitionAwareService implements PartitionAwareService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private ProxyRegistry mapProxyRegistry;

    public MapPartitionAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void onPartitionLost(InternalPartitionLostEvent partitionLostEvent) {
        final Address thisAddress = nodeEngine.getThisAddress();
        final int partitionId = partitionLostEvent.getPartitionId();

        LinkedList<DistributedObject> result = new LinkedList<DistributedObject>();
        getMapProxyRegistry().getDistributedObjects(result);

        for (DistributedObject object : result) {
            final MapProxyImpl mapProxy = (MapProxyImpl) object;
            final String mapName = mapProxy.getName();

            if (mapProxy.getBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
                mapServiceContext.getMapEventPublisher().publishMapPartitionLostEvent(thisAddress, mapName, partitionId);
            }
        }
    }

    private ProxyRegistry getMapProxyRegistry() {
        if (mapProxyRegistry == null) {
            mapProxyRegistry = ((ProxyServiceImpl) nodeEngine.getProxyService()).getOrCreateRegistry(MapService.SERVICE_NAME);
        }
        return mapProxyRegistry;
    }
}
