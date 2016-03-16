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

import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.tuple.TupleIterator;
import com.hazelcast.jet.impl.strategy.CalculationStrategyImpl;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.data.tuple.TupleConvertor;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class HazelcastMultiMapPartitionReader<K, V> extends AbstractHazelcastReader<Tuple<K, V>> {
    private final CalculationStrategy calculationStrategy;
    private final TupleConvertor<Map.Entry<Data, MultiMapValue>, K, V> tupleConverter =
            new TupleConvertor<Map.Entry<Data, MultiMapValue>, K, V>() {
                @Override
                public Tuple<K, V> convert(final Map.Entry<Data, MultiMapValue> entry, SerializationService ss) {
                    K key = ss.toObject(entry.getKey());
                    Collection<MultiMapRecord> multiMapRecordList = entry.getValue().getCollection(false);
                    List<MultiMapRecord> list;

                    if (multiMapRecordList instanceof List) {
                        list = (List<MultiMapRecord>) multiMapRecordList;
                    } else {
                        throw new IllegalStateException("Only list multimap is supported");
                    }

                    Object[] values = new Object[]{multiMapRecordList.size()};

                    for (int idx = 0; idx < list.size(); idx++) {
                        values[idx] = list.get(0).getObject();
                    }

                    return tupleFactory.tuple(
                            (K[]) new Object[]{key},
                            (V[]) values,
                            getPartitionId(),
                            calculationStrategy
                    );
                }
            };

    public HazelcastMultiMapPartitionReader(ContainerDescriptor containerDescriptor,
                                            String name,
                                            int partitionId,
                                            TupleFactory tupleFactory,
                                            Vertex vertex) {
        super(
                containerDescriptor,
                name,
                partitionId,
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

    @Override
    protected void onClose() {

    }

    @Override
    public void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) this.nodeEngine;
        SerializationService ss = nei.getSerializationService();
        MultiMapService multiMapService = nei.getService(MultiMapService.SERVICE_NAME);
        MultiMapContainer multiMapContainer = multiMapService.getPartitionContainer(
                getPartitionId()
        ).getCollectionContainer(getName());
        Iterator<Map.Entry<Data, MultiMapValue>> it = multiMapContainer.getMultiMapValues().entrySet().iterator();
        this.iterator = new TupleIterator<Map.Entry<Data, MultiMapValue>, K, V>(it, tupleConverter, ss);
    }

    @Override
    public boolean readFromPartitionThread() {
        return true;
    }
}
