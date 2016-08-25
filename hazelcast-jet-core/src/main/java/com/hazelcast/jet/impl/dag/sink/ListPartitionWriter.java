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


import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.container.ContainerContext;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.impl.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class ListPartitionWriter extends AbstractHazelcastWriter {

    private final String name;
    private final ListContainer listContainer;
    private final CalculationStrategy calculationStrategy;

    public ListPartitionWriter(ContainerContext containerContext, String name) {
        super(containerContext, getPartitionId(name, containerContext.getNodeEngine()));
        this.name = name;
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        ListService service = nodeEngine.getService(ListService.SERVICE_NAME);
        this.listContainer = service.getOrCreateContainer(name, false);
        this.calculationStrategy = new CalculationStrategy(
                SerializedHashingStrategy.INSTANCE, getPartitionStrategy(), containerContext);
    }

    private static int getPartitionId(String name, NodeEngine nodeEngine) {
        Data data = nodeEngine.getSerializationService().toData(name, StringPartitioningStrategy.INSTANCE);
        return nodeEngine.getPartitionService().getPartitionId(data);
    }

    @Override
    protected void processChunk(ProducerInputStream<Object> chunk) {
        for (int i = 0; i < chunk.size(); i++) {
            final JetPair pair = (JetPair) chunk.get(i);
            if (pair == null) {
                continue;
            }
            if (!listContainer.hasEnoughCapacity(chunk.size())) {
                throw new IllegalStateException("IList " + name + " capacity exceeded");
            }
            if (!(pair.get(0) instanceof Number)) {
                throw new IllegalStateException("The key of an IList pair should be a number");
            }
            this.listContainer.add(pair.getComponentData(1, calculationStrategy, getNodeEngine().getSerializationService()));
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}
