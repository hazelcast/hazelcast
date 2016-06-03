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

package com.hazelcast.jet.impl.dag.tap.source;

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.impl.data.tuple.JetTupleIterator;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.JetTuple;
import com.hazelcast.jet.data.tuple.JetTupleConvertor;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.List;

public class HazelcastListPartitionReader<K, V> extends AbstractHazelcastReader<JetTuple<K, V>> {
    private final CalculationStrategy calculationStrategy;
    private final JetTupleConvertor<CollectionItem, K, V> tupleConverter = new JetTupleConvertor<CollectionItem, K, V>() {
        @Override
        public JetTuple<K, V> convert(CollectionItem item, SerializationService ss) {
            return tupleFactory.tuple(
                    (K[]) new Object[]{item.getItemId()},
                    (V[]) new Object[]{ss.toObject(item.getValue())},
                    getPartitionId(),
                    calculationStrategy
            );
        }
    };

    public HazelcastListPartitionReader(ContainerDescriptor containerDescriptor,
                                        String name,
                                        JetTupleFactory tupleFactory,
                                        Vertex vertex) {
        super(containerDescriptor,
                name,
                getPartitionId(containerDescriptor.getNodeEngine(), name),
                tupleFactory,
                vertex,
                ByReferenceDataTransferringStrategy.INSTANCE
        );
        this.calculationStrategy = new CalculationStrategyImpl(
                DefaultHashingStrategy.INSTANCE,
                StringAndPartitionAwarePartitioningStrategy.INSTANCE,
                containerDescriptor
        );
    }

    public static int getPartitionId(NodeEngine nodeEngine, String name) {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        SerializationService ss = nei.getSerializationService();
        IPartitionService ps = nei.getPartitionService();
        Data data = ss.toData(name, StringPartitioningStrategy.INSTANCE);
        return ps.getPartitionId(data);
    }

    @Override
    protected void onClose() {

    }

    protected void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        ListService listService = nei.getService(ListService.SERVICE_NAME);
        ListContainer listContainer = listService.getOrCreateContainer(getName(), false);
        List<CollectionItem> items = listContainer.getCollection();
        SerializationService ss = nei.getSerializationService();
        this.iterator = new JetTupleIterator<CollectionItem, K, V>(items.iterator(), tupleConverter, ss);
    }

    @Override
    public boolean readFromPartitionThread() {
        return true;
    }
}
