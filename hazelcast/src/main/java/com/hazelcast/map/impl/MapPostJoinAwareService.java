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

import com.hazelcast.map.impl.operation.PostJoinMapOperation;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.PostJoinAwareService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

class MapPostJoinAwareService implements PostJoinAwareService {

    private final MapServiceContext mapServiceContext;

    MapPostJoinAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public Operation getPostJoinOperation() {
        PostJoinMapOperation postJoinOp = new PostJoinMapOperation();
        final Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        for (MapContainer mapContainer : mapContainers.values()) {
            postJoinOp.addMapInterceptors(mapContainer);
        }
        List<AccumulatorInfo> infoList = getAccumulatorInfoList();
        postJoinOp.setInfoList(infoList);
        postJoinOp.setNodeEngine(mapServiceContext.getNodeEngine());
        return postJoinOp;
    }

    private List<AccumulatorInfo> getAccumulatorInfoList() {
        List<AccumulatorInfo> infoList = new ArrayList<>();

        PublisherContext publisherContext = mapServiceContext.getQueryCacheContext().getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        Map<String, PublisherRegistry> cachesOfMaps = mapPublisherRegistry.getAll();
        Collection<PublisherRegistry> publisherRegistries = cachesOfMaps.values();
        for (PublisherRegistry publisherRegistry : publisherRegistries) {
            Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = publisherRegistry.getAll().values();
            for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
                AccumulatorInfo info = accumulatorRegistry.getInfo();
                infoList.add(info);
            }
        }
        return infoList;
    }
}
