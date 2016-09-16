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

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.jet.impl.ringbuffer.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.pair.JetPairConverter;
import com.hazelcast.jet.impl.data.pair.JetPairIterator;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.runtime.JetPair;

import java.util.Iterator;
import java.util.List;

public class ListPartitionProducer extends AbstractHazelcastProducer<JetPair> {
    private final JetPairConverter<CollectionItem> pairConverter =
            (item, ss) -> new JetPair<>(item.getItemId(), ss.toObject(item.getValue()), getPartitionId());

    public ListPartitionProducer(JobContext jobContext, String name, int partitionId) {
        super(jobContext, name, partitionId, ByReferenceDataTransferringStrategy.INSTANCE);
    }

    @Override
    public boolean mustRunOnPartitionThread() {
        return true;
    }

    @Override
    protected Iterator<JetPair> newIterator() {
        ListService listService = nodeEngine.getService(ListService.SERVICE_NAME);
        List<CollectionItem> items = listService.getOrCreateContainer(getName(), false).getCollection();
        return new JetPairIterator<>(items.iterator(), pairConverter, nodeEngine.getSerializationService());
    }

    @Override
    public void close() {
    }
}
