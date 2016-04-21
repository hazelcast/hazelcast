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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.data.tuple.JetTuple;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class HazelcastMapPartitionWriter extends AbstractHazelcastWriter {
    private final MapConfig mapConfig;
    private final RecordStore recordStore;
    private final CalculationStrategy calculationStrategy;

    public HazelcastMapPartitionWriter(ContainerDescriptor containerDescriptor,
                                       int partitionId,
                                       SinkTapWriteStrategy sinkTapWriteStrategy,
                                       String name) {
        super(containerDescriptor, partitionId, sinkTapWriteStrategy);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerDescriptor.getNodeEngine();
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        this.recordStore = mapServiceContext.getPartitionContainer(getPartitionId()).getRecordStore(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();

        if (partitioningStrategy == null) {
            partitioningStrategy = StringPartitioningStrategy.INSTANCE;
        }

        this.calculationStrategy = new CalculationStrategyImpl(
                DefaultHashingStrategy.INSTANCE,
                partitioningStrategy,
                containerDescriptor
        );
    }

    @Override
    protected void processChunk(ProducerInputStream<Object> chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            JetTuple tuple = (JetTuple) chunk.get(i);

            Object dataKey;
            Object dataValue;

            dataKey = tuple.getKeyData(this.calculationStrategy, getNodeEngine());

            if (mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY) {
                dataValue = tuple.getValueData(this.calculationStrategy, getNodeEngine());
            } else {
                dataValue = tuple.valueSize() == 1 ? tuple.getValue(0) : tuple.cloneValues();
            }

            this.recordStore.put((Data) dataKey, dataValue, -1);
        }
    }

    @Override
    protected void onOpen() {
        this.recordStore.clear();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.calculationStrategy.getPartitioningStrategy();
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
