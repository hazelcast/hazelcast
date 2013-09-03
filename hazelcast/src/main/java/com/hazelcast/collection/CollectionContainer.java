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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ali 8/30/13
 */
public abstract class CollectionContainer implements DataSerializable {

    protected String name;
    protected int partitionId;
    protected NodeEngine nodeEngine;
    protected CollectionService service;
    protected ILogger logger;

    protected final Map<Long, CollectionItem> txMap = new HashMap<Long, CollectionItem>();

    private long idGenerator = 0;

    protected CollectionContainer() {
    }

    protected CollectionContainer(String name, NodeEngine nodeEngine, CollectionService service) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.getSerializationService().toData(name, CollectionService.PARTITIONING_STRATEGY));
        logger = nodeEngine.getLogger(getClass());
    }

    protected abstract CollectionItem remove(Data value);
    protected abstract void removeBackup(long itemId);

    protected abstract int size();

    protected abstract Map<Long, Data> clear();
    protected abstract void clearBackup(Set<Long> itemIdSet);

    protected abstract boolean contains(Set<Data> valueSet);

    protected abstract Map<Long, Data> addAll(List<Data> valueList);
    protected abstract void addAllBackup(Map<Long, Data> valueMap);

    protected abstract Map<Long, Data> compareAndRemove(boolean retain, Set<Data> valueSet);

    public long reserveAdd(){
        final long itemId = nextId();
        txMap.put(itemId, new CollectionItem(this, itemId, null));
        return itemId;
    }

    public abstract CollectionItem reserveRemove(long reservedItemId, Data value);

    public void reserveAddBackup(long itemId) {
        CollectionItem item = new CollectionItem(this, itemId, null);
        Object o = txMap.put(itemId, item);
        if (o != null) {
            logger.severe("txnOfferBackupReserve operation-> Item exists already at txMap for itemId: " + itemId);
        }
    }

    public abstract void reserveRemoveBackup(long itemId);

    public void ensureReserve(long itemId) {
        if (txMap.get(itemId) == null) {
            throw new TransactionException("No reserve for itemId: " + itemId);
        }
    }

    public void rollbackAdd(long itemId){
        if (txMap.remove(itemId) == null) {
            logger.warning("rollbackAdd operation-> No txn item for itemId: " + itemId);
        }
    }

    public void rollbackAddBackup(long itemId){
        if (txMap.remove(itemId) == null) {
            logger.warning("rollbackAddBackup operation-> No txn item for itemId: " + itemId);
        }
    }

    public abstract void rollbackRemove(long itemId);
    public abstract void rollbackRemoveBackup(long itemId);

    public abstract void commitAdd(long itemId, Data value);
    public abstract void commitAddBackup(long itemId, Data value);

    public abstract void commitRemove(long itemId);
    public abstract void commitRemoveBackup(long itemId);



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
