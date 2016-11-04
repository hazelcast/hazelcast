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

import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.impl.ringbuffer.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.pair.JetPairConverter;
import com.hazelcast.jet.impl.data.pair.JetPairIterator;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class MultiMapPartitionProducer extends AbstractHazelcastProducer<JetPair> {
    private final JetPairConverter<Entry<Data, MultiMapValue>> pairConverter =
            new JetPairConverter<Entry<Data, MultiMapValue>>() {
                @Override
                public JetPair convert(final Entry<Data, MultiMapValue> entry, SerializationService ss) {
                    Object key = ss.toObject(entry.getKey());
                    Collection<MultiMapRecord> multiMapRecordList = entry.getValue().getCollection(false);
                    List<MultiMapRecord> list;
                    if (multiMapRecordList instanceof List) {
                        list = (List<MultiMapRecord>) multiMapRecordList;
                    } else {
                        throw new IllegalStateException("Only list multimap is supported");
                    }
                    Object[] values = new Object[multiMapRecordList.size()];
                    for (int idx = 0; idx < list.size(); idx++) {
                        values[idx] = list.get(0).getObject();
                    }
                    return new JetPair<>(key, values, getPartitionId());
                }
            };

    public MultiMapPartitionProducer(JobContext jobContext, String name, int partitionId) {
        super(jobContext, name, partitionId, ByReferenceDataTransferringStrategy.INSTANCE);
    }

    @Override
    public Iterator<JetPair> newIterator() {
        NodeEngineImpl nei = (NodeEngineImpl) this.nodeEngine;
        SerializationService ss = nei.getSerializationService();
        MultiMapService multiMapService = nei.getService(MultiMapService.SERVICE_NAME);
        MultiMapContainer multiMapContainer = multiMapService.getPartitionContainer(getPartitionId())
                .getCollectionContainer(getName());
        Iterator<Map.Entry<Data, MultiMapValue>> it = multiMapContainer.getMultiMapValues().entrySet().iterator();
        return new JetPairIterator<>(it, pairConverter, ss);
    }

    @Override
    public boolean mustRunOnPartitionThread() {
        return true;
    }

    @Override
    public void close() {
    }
}
