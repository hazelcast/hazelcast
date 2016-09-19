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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;

public class MultiMapPartitionWriter extends AbstractHazelcastWriter {
    private final MultiMapContainer container;
    private final CalculationStrategy calculationStrategy;

    public MultiMapPartitionWriter(JobContext jobContext, int partitionId, String name) {
        super(jobContext, partitionId);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) jobContext.getNodeEngine();
        MultiMapService service = nodeEngine.getService(MultiMapService.SERVICE_NAME);
        this.container = service.getOrCreateCollectionContainer(getPartitionId(), name);
        this.calculationStrategy = new CalculationStrategy(SerializedHashingStrategy.INSTANCE,
                StringPartitioningStrategy.INSTANCE, jobContext);
    }

    @Override
    protected void processChunk(InputChunk inputChunk) {
        for (int i = 0; i < inputChunk.size(); i++) {
            JetPair<Object, Object[]> pair = (JetPair) inputChunk.get(i);
            Data dataKey = pair.getComponentData(0, calculationStrategy, getNodeEngine().getSerializationService());
            Collection<MultiMapRecord> coll = container.getMultiMapValueOrNull(dataKey).getCollection(false);
            long recordId = container.nextId();
            for (Object value : pair.getValue()) {
                Data dataValue = getNodeEngine().getSerializationService().toData(value);
                MultiMapRecord record = new MultiMapRecord(recordId, dataValue);
                coll.add(record);
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
