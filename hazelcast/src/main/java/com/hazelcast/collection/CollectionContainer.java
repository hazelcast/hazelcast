/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @ali 8/30/13
 */
public abstract class CollectionContainer implements DataSerializable {

    private String name;
    private int partitionId;
    private NodeEngine nodeEngine;
    private CollectionService service;

    private long idGenerator = 0;

    protected CollectionContainer() {
    }

    protected CollectionContainer(String name, NodeEngine nodeEngine, CollectionService service) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.getSerializationService().toData(name, CollectionService.PARTITIONING_STRATEGY));
    }

    protected abstract CollectionItem remove(Data value);
    protected abstract void removeBackup(long itemId);

    protected abstract int size();

    protected abstract Set<Long> clear();
    protected abstract void clearBackup(Set<Long> itemIdSet);

    protected abstract boolean contains(Set<Data> valueSet);

    protected abstract Map<Long, Data> addAll(Set<Data> valueSet);
    protected abstract void addAllBackup(Map<Long, Data> valueMap);

    protected abstract Set<Long> compareAndRemove(boolean retain, Set<Data> valueSet);

    public long nextId() {
        return idGenerator++;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
    }
}
