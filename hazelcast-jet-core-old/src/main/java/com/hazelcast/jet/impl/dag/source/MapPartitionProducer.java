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
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.impl.ringbuffer.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.pair.JetPairConverter;
import com.hazelcast.jet.impl.data.pair.JetPairIterator;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;

public class MapPartitionProducer extends AbstractHazelcastProducer<JetPair> {
    private final MapConfig mapConfig;

    private final JetPairConverter<Record> pairConverter = new JetPairConverter<Record>() {
        @Override
        public JetPair convert(Record record, SerializationService ss) {
            final Object value = mapConfig.getInMemoryFormat() == InMemoryFormat.BINARY
                    ? ss.toObject(record.getValue()) : record.getValue();
            return new JetPair<>(ss.toObject(record.getKey()), value, getPartitionId());
        }
    };

    public MapPartitionProducer(JobContext jobContext, String name, int partitionId) {
        super(jobContext, name, partitionId, ByReferenceDataTransferringStrategy.INSTANCE);
        NodeEngineImpl nodeEngine = (NodeEngineImpl) jobContext.getNodeEngine();
        this.mapConfig = nodeEngine.getConfig().getMapConfig(name);
    }

    @Override
    public Iterator<JetPair> newIterator() {
        final NodeEngineImpl nei = (NodeEngineImpl) this.nodeEngine;
        final SerializationService ss = nei.getSerializationService();
        final MapService mapService = nei.getService(MapService.SERVICE_NAME);
        final PartitionContainer pc = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        final RecordStore<Record> recordStore = pc.getRecordStore(getName());
        return new JetPairIterator<>(recordStore.iterator(), pairConverter, ss);
    }

    @Override
    public boolean mustRunOnPartitionThread() {
        return true;
    }

    @Override
    public void close() {
    }
}
