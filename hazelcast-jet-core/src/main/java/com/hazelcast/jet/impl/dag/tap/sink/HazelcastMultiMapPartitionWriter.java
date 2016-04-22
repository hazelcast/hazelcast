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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.data.tuple.JetTuple;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;

public class HazelcastMultiMapPartitionWriter extends AbstractHazelcastWriter {
    private final MultiMapContainer container;

    public HazelcastMultiMapPartitionWriter(ContainerDescriptor containerDescriptor,
                                            int partitionId,
                                            SinkTapWriteStrategy sinkTapWriteStrategy,
                                            String name) {
        super(containerDescriptor, partitionId, sinkTapWriteStrategy);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerDescriptor.getNodeEngine();
        MultiMapService service = nodeEngine.getService(MultiMapService.SERVICE_NAME);
        this.container = service.getOrCreateCollectionContainer(getPartitionId(), name);
    }

    @Override
    protected void processChunk(ProducerInputStream chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            Object object = chunk.get(i);
            if (object instanceof JetTuple) {
                JetTuple tuple = (JetTuple) object;
                Data dataKey = tuple.getKeyData(getNodeEngine());
                Collection<MultiMapRecord> coll = this.container.getMultiMapValueOrNull(dataKey).getCollection(false);
                long recordId = this.container.nextId();

                for (int idx = 0; idx < tuple.valueSize(); idx++) {
                    Data dataValue = getNodeEngine().getSerializationService().toData(tuple.getValue(idx));
                    MultiMapRecord record = new MultiMapRecord(recordId, dataValue);
                    coll.add(record);
                }
            }
        }
    }

    @Override
    protected void onOpen() {
        this.container.clear();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }
}
