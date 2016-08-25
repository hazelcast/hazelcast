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
import com.hazelcast.collection.impl.list.ListContainer;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.jet.container.ContainerContext;
import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.data.pair.JetPairConverter;
import com.hazelcast.jet.impl.data.pair.JetPairIterator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Iterator;
import java.util.List;

public class ListPartitionReader extends AbstractHazelcastReader<JetPair> {
    private final JetPairConverter<CollectionItem> pairConverter =
            (item, ss) -> new JetPair<>(item.getItemId(), ss.toObject(item.getValue()), getPartitionId());

    public ListPartitionReader(ContainerContext containerContext, String name) {
        super(containerContext, name, getPartitionId(containerContext.getNodeEngine(), name),
                ByReferenceDataTransferringStrategy.INSTANCE);
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

    @Override
    protected void onOpen() {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        ListService listService = nei.getService(ListService.SERVICE_NAME);
        ListContainer listContainer = listService.getOrCreateContainer(getName(), false);
        List<CollectionItem> items = listContainer.getCollection();
        SerializationService ss = nei.getSerializationService();
        final Iterator<CollectionItem> iter = items.iterator();
        this.iterator = new JetPairIterator<>(iter, pairConverter, ss);
    }

    @Override
    public boolean readFromPartitionThread() {
        return true;
    }
}
