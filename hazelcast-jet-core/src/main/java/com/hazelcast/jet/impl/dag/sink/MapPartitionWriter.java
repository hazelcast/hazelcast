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

package com.hazelcast.jet.impl.dag.sink;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

public class MapPartitionWriter extends AbstractHazelcastWriter {
    private final MapConfig mapConfig;
    private final RecordStore recordStore;
    private final CalculationStrategy calculationStrategy;

    public MapPartitionWriter(JobContext jobContext, int partitionId, String name) {
        super(jobContext, partitionId);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) jobContext.getNodeEngine();
        // make sure proxy for the map is created
        nodeEngine.getHazelcastInstance().getMap(name);

        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        this.recordStore = mapServiceContext.getPartitionContainer(getPartitionId()).getRecordStore(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
        if (partitioningStrategy == null) {
            partitioningStrategy = StringAndPartitionAwarePartitioningStrategy.INSTANCE;
        }
        this.calculationStrategy = new CalculationStrategy(SerializedHashingStrategy.INSTANCE,
                partitioningStrategy, jobContext);
    }

    @Override
    protected void processChunk(InputChunk<Object> inputChunk) {
        for (int i = 0; i < inputChunk.size(); i++) {
            JetPair pair = (JetPair) inputChunk.get(i);
            final SerializationService serService = getNodeEngine().getSerializationService();
            final Data keyData = pair.getComponentData(0, calculationStrategy, serService);
            final Object valueData = mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY
                    ? pair.getComponentData(1, calculationStrategy, serService) : pair.getValue();
            this.recordStore.put(keyData, valueData, -1);
        }
    }

    @Override
    protected void onOpen() {
        recordStore.clear();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return calculationStrategy.getPartitioningStrategy();
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
