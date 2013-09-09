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
import java.util.*;

/**
 * @ali 8/30/13
 */
public abstract class CollectionContainer implements DataSerializable {

    protected String name;
    protected int partitionId;
    protected NodeEngine nodeEngine;
    protected CollectionService service;
    protected ILogger logger;

    protected final Map<Long, TxCollectionItem> txMap = new HashMap<Long, TxCollectionItem>();

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

    protected abstract Collection<CollectionItem> getCollection();
    protected abstract Map<Long, CollectionItem> getMap();

    protected long add(Data value){
        final CollectionItem item = new CollectionItem(this, nextId(), value);
        if(getCollection().add(item)){
            return item.getItemId();
        }
        return -1;
    }

    protected void addBackup(long itemId, Data value){
        final CollectionItem item = new CollectionItem(this, itemId, value);
        getMap().put(itemId, item);
    }

    protected CollectionItem remove(Data value) {
        final Iterator<CollectionItem> iterator = getCollection().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())){
                iterator.remove();
                return item;
            }
        }
        return null;
    }

    protected void removeBackup(long itemId) {
        getMap().remove(itemId);
    }

    protected int size() {
        return getCollection().size();
    }

    protected Map<Long, Data> clear() {
        final Collection<CollectionItem> coll = getCollection();
        Map<Long, Data> itemIdMap = new HashMap<Long, Data>(coll.size());
        for (CollectionItem item : coll) {
            itemIdMap.put(item.getItemId(), (Data) item.getValue());
        }
        coll.clear();
        return itemIdMap;
    }

    protected void clearBackup(Set<Long> itemIdSet) {
        for (Long itemId : itemIdSet) {
            removeBackup(itemId);
        }
    }

    protected boolean contains(Set<Data> valueSet) {
        for (Data value : valueSet) {
            boolean contains = false;
            for (CollectionItem item : getCollection()) {
                if (value.equals(item.getValue())){
                    contains = true;
                    break;
                }
            }
            if (!contains){
                return false;
            }
        }
        return true;
    }

    protected Map<Long, Data> addAll(List<Data> valueList) {
        final int size = valueList.size();
        final Map<Long, Data> map = new HashMap<Long, Data>(size);
        List<CollectionItem> list = new ArrayList<CollectionItem>(size);
        for (Data value : valueList) {
            final long itemId = nextId();
            list.add(new CollectionItem(this, itemId, value));
            map.put(itemId, value);
        }
        getCollection().addAll(list);

        return map;
    }

    protected void addAllBackup(Map<Long, Data> valueMap) {
        Map<Long, CollectionItem> map = new HashMap<Long, CollectionItem>(valueMap.size());
        for (Map.Entry<Long, CollectionItem> entry : map.entrySet()) {
            final long itemId = entry.getKey();
            map.put(itemId, new CollectionItem(this, itemId, entry.getValue()));
        }
        getMap().putAll(map);
    }

    protected Map<Long, Data> compareAndRemove(boolean retain, Set<Data> valueSet) {
        Map<Long, Data> itemIdMap = new HashMap<Long, Data>();
        final Iterator<CollectionItem> iterator = getCollection().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            final boolean contains = valueSet.contains(item.getValue());
            if ( (contains && !retain) || (!contains && retain)){
                itemIdMap.put(item.getItemId(), (Data) item.getValue());
                iterator.remove();
            }
        }
        return itemIdMap;
    }

    protected Collection<Data> getAll(){
        final ArrayList<Data> sub = new ArrayList<Data>(getCollection().size());
        for (CollectionItem item : getCollection()) {
            sub.add((Data)item.getValue());
        }
        return sub;
    }


    /*
     * TX methods
     *
     */

    public long reserveAdd(String transactionId){
        final long itemId = nextId();
        txMap.put(itemId, new TxCollectionItem(this, itemId, null, transactionId, false));
        return itemId;
    }

    public void reserveAddBackup(long itemId, String transactionId) {
        TxCollectionItem item = new TxCollectionItem(this, itemId, null, transactionId, false);
        Object o = txMap.put(itemId, item);
        if (o != null) {
            logger.severe("txnOfferBackupReserve operation-> Item exists already at txMap for itemId: " + itemId);
        }
    }

    public CollectionItem reserveRemove(long reservedItemId, Data value, String transactionId) {
        final Iterator<CollectionItem> iterator = getCollection().iterator();
        while (iterator.hasNext()){
            final CollectionItem item = iterator.next();
            if (value.equals(item.getValue())){
                iterator.remove();
                txMap.put(item.getItemId(), new TxCollectionItem(item).setTransactionId(transactionId).setRemoveOperation(true));
                return item;
            }
        }
        if (reservedItemId != -1){
            return txMap.remove(reservedItemId);
        }
        return null;
    }

    public void reserveRemoveBackup(long itemId, String transactionId) {
        final CollectionItem item = getMap().remove(itemId);
        if (item == null) {
            throw new TransactionException("Backup reserve failed: " + itemId);
        }
        txMap.put(itemId, new TxCollectionItem(item).setTransactionId(transactionId).setRemoveOperation(true));
    }

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

    public void rollbackRemove(long itemId) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("rollbackRemove No txn item for itemId: " + itemId);
        }
        getCollection().add(item);
    }

    public void rollbackRemoveBackup(long itemId) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("rollbackRemoveBackup No txn item for itemId: " + itemId);
        }
    }

    public void commitAdd(long itemId, Data value) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            throw new TransactionException("No reserve :" + itemId);
        }
        item.setValue(value);
        getCollection().add(item);
    }

    public void commitAddBackup(long itemId, Data value) {
        CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            item = new CollectionItem(this, itemId, value);
        }
        getMap().put(itemId, item);
    }

    public CollectionItem commitRemove(long itemId) {
        final CollectionItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("commitRemove operation-> No txn item for itemId: " + itemId);
        }
        return item;
    }

    public void commitRemoveBackup(long itemId) {
        if (txMap.remove(itemId) == null) {
            logger.warning("commitRemoveBackup operation-> No txn item for itemId: " + itemId);
        }
    }

    public void rollbackTransaction(String transactionId) {
        final Iterator<TxCollectionItem> iterator = txMap.values().iterator();

        while (iterator.hasNext()){
            final TxCollectionItem item = iterator.next();
            if (transactionId.equals(item.getTransactionId())){
                iterator.remove();
                if (item.isRemoveOperation()){
                    getCollection().add(item);
                }
            }
        }
    }

    public long nextId() {
        return idGenerator++;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);

        //TODO @ali migration
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
    }


}
