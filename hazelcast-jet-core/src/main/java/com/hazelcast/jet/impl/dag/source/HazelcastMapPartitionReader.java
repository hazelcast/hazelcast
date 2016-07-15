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

package com.hazelcast.jet.impl.dag.source;


import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.tuple.JetTuple;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.tuple.JetTupleConverter;
import com.hazelcast.jet.impl.data.tuple.JetTupleIterator;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

public class HazelcastMapPartitionReader extends AbstractHazelcastReader<JetTuple> {
    private final MapConfig mapConfig;
    private final CalculationStrategy calculationStrategy;

    private final JetTupleConverter<Record> tupleConverter = new JetTupleConverter<Record>() {
        @Override
        public JetTuple2 convert(Record record, SerializationService ss) {
            final Object value = mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY
                    ? ss.toObject(record.getValue()) : record.getValue();
            return tupleFactory.tuple2(ss.toObject(record.getKey()), value, getPartitionId(), calculationStrategy);
        }
    };

    public HazelcastMapPartitionReader(
            ContainerDescriptor containerDescriptor, String name, int partitionId, JetTupleFactory tupleFactory
    ) {
        super(containerDescriptor, name, partitionId, tupleFactory, ByReferenceDataTransferringStrategy.INSTANCE);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerDescriptor.getNodeEngine();
        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        PartitioningStrategy partitioningStrategy = mapContainer.getPartitioningStrategy();
        partitioningStrategy = partitioningStrategy == null ? StringPartitioningStrategy.INSTANCE : partitioningStrategy;
        this.calculationStrategy = new CalculationStrategyImpl(
                DefaultHashingStrategy.INSTANCE, partitioningStrategy, this.containerDescriptor);
    }

    @Override
    public void onOpen() {
        final NodeEngineImpl nei = (NodeEngineImpl) this.nodeEngine;
        final SerializationService ss = nei.getSerializationService();
        final MapService mapService = nei.getService(MapService.SERVICE_NAME);
        final PartitionContainer pc = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        final RecordStore<Record> recordStore = pc.getRecordStore(getName());
        this.iterator = new JetTupleIterator<>(recordStore.iterator(), tupleConverter, ss);
    }

    @Override
    public boolean readFromPartitionThread() {
        return true;
    }

    @Override
    protected void onClose() {
    }
}
