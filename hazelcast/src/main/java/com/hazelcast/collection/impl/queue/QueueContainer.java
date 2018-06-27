/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.collection.impl.txnqueue.TxQueueItem;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.collection.impl.collection.CollectionContainer.ID_PROMOTION_OFFSET;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.MapUtil.createLinkedHashMap;
import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * The {@code QueueContainer} contains the actual queue and provides functionalities such as :
 * <ul>
 * <li>queue functionalities</li>
 * <li>transactional operation functionalities</li>
 * <li>schedules queue destruction if it is configured to be destroyed once empty</li>
 * </ul>
 */
@SuppressWarnings("checkstyle:methodcount")
public class QueueContainer implements IdentifiedDataSerializable {

    /**
     * Contains item ID to queue item mappings for current transactions
     */
    private final Map<Long, TxQueueItem> txMap = new HashMap<Long, TxQueueItem>();
    private final Map<Long, Data> dataMap = new HashMap<Long, Data>();
    private QueueWaitNotifyKey pollWaitNotifyKey;
    private QueueWaitNotifyKey offerWaitNotifyKey;
    private LinkedList<QueueItem> itemQueue;
    private Map<Long, QueueItem> backupMap;
    private QueueConfig config;
    private QueueStoreWrapper store;
    private NodeEngine nodeEngine;
    private QueueService service;
    private ILogger logger;
    /**
     * The ID of the last item, used for generating unique IDs for queue items
     */
    private long idGenerator;
    private String name;

    private long minAge = Long.MAX_VALUE;
    private long maxAge = Long.MIN_VALUE;
    private long totalAge;
    private long totalAgedCount;
    private boolean isEvictionScheduled;
    // when QueueStore is configured & enabled, stores the last item ID that was bulk-loaded by QueueStore.loadAll
    // to avoid reloading same items
    private long lastIdLoaded;

    /**
     * The default no-args constructor is only meant for factory usage.
     */
    public QueueContainer() {
    }

    public QueueContainer(String name) {
        this.name = name;
        this.pollWaitNotifyKey = new QueueWaitNotifyKey(name, "poll");
        this.offerWaitNotifyKey = new QueueWaitNotifyKey(name, "offer");
    }

    public QueueContainer(String name, QueueConfig config, NodeEngine nodeEngine, QueueService service) {
        this(name);
        setConfig(config, nodeEngine, service);
    }

    /**
     * Initializes the item queue with items from the queue store if the store is enabled and if item queue is not being
     * initialized as a part of a backup operation. If the item queue is being initialized as a part of a backup operation then
     * the operation is in charge of adding items to a queue and the items are not loaded from a queue store.
     *
     * @param fromBackup indicates if this item queue is being initialized from a backup operation. If false, the
     *                   item queue will initialize from the queue store. If true, it will not initialize
     */
    public void init(boolean fromBackup) {
        if (!fromBackup && store.isEnabled()) {
            Set<Long> keys = store.loadAllKeys();
            if (keys != null) {
                long maxId = -1;
                for (Long key : keys) {
                    QueueItem item = new QueueItem(this, key, null);
                    getItemQueue().offer(item);
                    maxId = Math.max(maxId, key);
                }
                idGenerator = maxId + 1;
            }
        }
    }

    public QueueStoreWrapper getStore() {
        return store;
    }

    public String getName() {
        return name;
    }

    // TX Methods

    /**
     * Checks if there is a reserved item (within a transaction) with the given {@code itemId}.
     *
     * @param itemId the ID which is to be checked
     * @return true if there is an item reserved with this ID
     * @throws TransactionException if there is no reserved item with the ID
     */
    public boolean txnCheckReserve(long itemId) {
        if (txMap.get(itemId) == null) {
            throw new TransactionException("No reserve for itemId: " + itemId);
        }
        return true;
    }

    public void txnEnsureBackupReserve(long itemId, String transactionId, boolean pollOperation) {
        if (txMap.get(itemId) == null) {
            if (pollOperation) {
                txnPollBackupReserve(itemId, transactionId);
            } else {
                txnOfferBackupReserve(itemId, transactionId);
            }
        }
    }

    // TX Poll

    /**
     * Tries to obtain an item by removing the head of the
     * queue or removing an item previously reserved by invoking
     * {@link #txnOfferReserve(String)} with {@code reservedOfferId}.
     * <p>
     * If the queue item does not have data in-memory it will load the
     * data from the queue store if the queue store is configured and enabled.
     *
     * @param reservedOfferId the ID of the reserved item to be returned if the queue is empty
     * @param transactionId   the transaction ID for which this poll is invoked
     * @return the head of the queue or a reserved item with the {@code reservedOfferId} if there is any
     */
    public QueueItem txnPollReserve(long reservedOfferId, String transactionId) {
        QueueItem item = getItemQueue().peek();
        if (item == null) {
            TxQueueItem txItem = txMap.remove(reservedOfferId);
            if (txItem == null) {
                return null;
            }
            item = new QueueItem(this, txItem.getItemId(), txItem.getData());
            return item;
        }
        if (store.isEnabled() && item.getData() == null) {
            try {
                load(item);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        getItemQueue().poll();
        txMap.put(item.getItemId(), new TxQueueItem(item).setPollOperation(true).setTransactionId(transactionId));
        return item;
    }

    /**
     * Makes a reservation for a poll operation. Should be executed as
     * a part of the prepare phase for a transactional queue poll
     * on the partition backup replica.
     * The ID of the item being polled is determined by the partition
     * owner.
     *
     * @param itemId        the ID of the reserved item to be polled
     * @param transactionId the transaction ID
     * @see #txnPollReserve(long, String)
     * @see com.hazelcast.collection.impl.txnqueue.operations.TxnReservePollOperation
     */
    public void txnPollBackupReserve(long itemId, String transactionId) {
        QueueItem item = getBackupMap().remove(itemId);
        if (item != null) {
            txMap.put(itemId, new TxQueueItem(item).setPollOperation(true).setTransactionId(transactionId));
            return;
        }
        if (txMap.remove(itemId) == null) {
            logger.warning("Poll backup reserve failed, itemId: " + itemId + " is not found");
        }
    }

    public Data txnCommitPoll(long itemId) {
        Data result = txnCommitPollBackup(itemId);
        scheduleEvictionIfEmpty();
        return result;
    }

    /**
     * Commits the effects of the {@link #txnPollReserve(long, String)}}. Also deletes the item data from the queue
     * data store if it is configured and enabled.
     *
     * @param itemId the ID of the item which was polled inside a transaction
     * @return the data of the polled item
     * @throws HazelcastException if there was any exception while removing the item from the queue data store
     */
    public Data txnCommitPollBackup(long itemId) {
        TxQueueItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("txnCommitPoll operation-> No txn item for itemId: " + itemId);
            return null;
        }
        if (store.isEnabled()) {
            try {
                store.delete(item.getItemId());
            } catch (Exception e) {
                logger.severe("Error during store delete: " + item.getItemId(), e);
            }
        }
        return item.getData();
    }

    /**
     * Rolls back the effects of the {@link #txnPollReserve(long, String)}.
     * The {@code backup} parameter defines whether this item was stored
     * on a backup queue or a primary queue.
     * It will return the item to the queue or backup map if it wasn't
     * offered as a part of the transaction.
     * Cancels the queue eviction if one is scheduled.
     *
     * @param itemId the ID of the item which was polled in a transaction
     * @param backup if this is the primary or the backup replica for this queue
     * @return if there was any polled item with the {@code itemId} inside a transaction
     */
    public boolean txnRollbackPoll(long itemId, boolean backup) {
        TxQueueItem item = txMap.remove(itemId);
        if (item == null) {
            return false;
        }

        if (backup) {
            getBackupMap().put(itemId, item);
        } else {
            addTxItemOrdered(item);
        }
        cancelEvictionIfExists();
        return true;
    }

    @SuppressWarnings("unchecked")
    private void addTxItemOrdered(TxQueueItem txQueueItem) {
        ListIterator<QueueItem> iterator = ((List<QueueItem>) getItemQueue()).listIterator();
        while (iterator.hasNext()) {
            QueueItem queueItem = iterator.next();
            if (txQueueItem.itemId < queueItem.itemId) {
                iterator.previous();
                break;
            }
        }
        iterator.add(txQueueItem);
    }

    // TX Offer

    /**
     * Reserves an ID for a future queue item and associates it with the given {@code transactionId}.
     * The item is not yet visible in the queue, it is just reserved for future insertion.
     *
     * @param transactionId the ID of the transaction offering this item
     * @return the ID of the reserved item
     */
    public long txnOfferReserve(String transactionId) {
        long itemId = nextId();
        txnOfferReserveInternal(itemId, transactionId);
        return itemId;
    }

    /**
     * Reserves an ID for a future queue item and associates it with the given {@code transactionId}.
     * The item is not yet visible in the queue, it is just reserved for future insertion.
     *
     * @param transactionId the ID of the transaction offering this item
     * @param itemId        the ID of the item being reserved
     */
    public void txnOfferBackupReserve(long itemId, String transactionId) {
        TxQueueItem o = txnOfferReserveInternal(itemId, transactionId);
        if (o != null) {
            logger.severe("txnOfferBackupReserve operation-> Item exists already at txMap for itemId: " + itemId);
        }
    }

    /**
     * Add a reservation for an item with {@code itemId} offered in transaction with {@code transactionId}
     */
    private TxQueueItem txnOfferReserveInternal(long itemId, String transactionId) {
        TxQueueItem item = new TxQueueItem(this, itemId, null)
                .setTransactionId(transactionId)
                .setPollOperation(false);
        return txMap.put(itemId, item);
    }

    /**
     * Sets the data of a reserved item and commits the change so it can be
     * visible outside a transaction.
     * The commit means that the item is offered to the queue if
     * {@code backup} is false or saved into a backup map if {@code backup} is {@code true}.
     * This is because a node can hold backups for queues on other nodes.
     * Cancels the queue eviction if one is scheduled.
     *
     * @param itemId the ID of the reserved item
     * @param data   the data to be associated with the reserved item
     * @param backup if the item is to be offered to the underlying queue or stored as a backup
     * @return {@code true} if the commit succeeded
     * @throws TransactionException if there is no reserved item with the {@code itemId}
     */
    public boolean txnCommitOffer(long itemId, Data data, boolean backup) {
        QueueItem item = txMap.remove(itemId);
        if (item == null && !backup) {
            throw new TransactionException("No reserve: " + itemId);
        } else if (item == null) {
            item = new QueueItem(this, itemId, data);
        }
        item.setData(data);
        if (!backup) {
            getItemQueue().offer(item);
            cancelEvictionIfExists();
        } else {
            getBackupMap().put(itemId, item);
        }
        if (store.isEnabled() && !backup) {
            try {
                store.store(item.getItemId(), data);
            } catch (Exception e) {
                logger.warning("Exception during store", e);
            }
        }
        return true;
    }

    /**
     * Removes a reserved item with the given {@code itemId}. Also schedules the queue for destruction if it is empty or
     * destroys it immediately if it is empty and {@link QueueConfig#getEmptyQueueTtl()} is 0.
     *
     * @param itemId the ID of the reserved item to be removed
     * @return if an item was reserved with the given {@code itemId}
     */
    public boolean txnRollbackOffer(long itemId) {
        boolean result = txnRollbackOfferBackup(itemId);
        scheduleEvictionIfEmpty();
        return result;
    }

    /**
     * Removes a reserved item with the given {@code itemId}.
     *
     * @param itemId the ID of the reserved item to be removed
     * @return if an item was reserved with the given {@code itemId}
     */
    public boolean txnRollbackOfferBackup(long itemId) {
        QueueItem item = txMap.remove(itemId);
        if (item == null) {
            logger.warning("txnRollbackOffer operation-> No txn item for itemId: " + itemId);
            return false;
        }
        return true;
    }

    /**
     * Retrieves, but does not remove, the head of the queue. If the queue is empty checks if there is a reserved item with
     * the associated {@code offerId} and returns it.
     * If the item was retrieved from the queue but does not contain any data and the queue store is enabled, this method will
     * try load the data from the data store.
     *
     * @param offerId       the ID of the reserved item to be returned if the queue is empty
     * @param transactionId currently ignored
     * @return the head of the queue or a reserved item associated with the {@code offerId} if the queue is empty
     * @throws HazelcastException if there is an exception while loading the data from the queue store
     */
    public QueueItem txnPeek(long offerId, String transactionId) {
        QueueItem item = getItemQueue().peek();
        if (item == null) {
            if (offerId == -1) {
                return null;
            }
            TxQueueItem txItem = txMap.get(offerId);
            if (txItem == null) {
                return null;
            }
            item = new QueueItem(this, txItem.getItemId(), txItem.getData());
            return item;
        }
        if (store.isEnabled() && item.getData() == null) {
            try {
                load(item);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        return item;
    }

    // TX Methods Ends

    public long offer(Data data) {
        QueueItem item = new QueueItem(this, nextId(), null);
        if (store.isEnabled()) {
            try {
                store.store(item.getItemId(), data);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
            item.setData(data);
        }
        getItemQueue().offer(item);
        cancelEvictionIfExists();
        return item.getItemId();
    }

    /**
     * Offers the item to the backup map. If the memory limit
     * has been achieved the item data will not be kept in-memory.
     * Executed on the backup replica
     *
     * @param data   the item data
     * @param itemId the item ID as determined by the primary replica
     */
    public void offerBackup(Data data, long itemId) {
        QueueItem item = new QueueItem(this, itemId, null);
        if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
            item.setData(data);
        }
        getBackupMap().put(itemId, item);
    }

    /**
     * Adds all items from the {@code dataList} to the queue. The data will be stored in the queue store if configured and
     * enabled. If the store is enabled, only {@link QueueStoreWrapper#getMemoryLimit()} item data will be stored in memory.
     * Cancels the eviction if one is scheduled.
     *
     * @param dataList the items to be added to the queue and stored in the queue store
     * @return map of item ID and items added
     */
    public Map<Long, Data> addAll(Collection<Data> dataList) {
        Map<Long, Data> map = createHashMap(dataList.size());
        List<QueueItem> list = new ArrayList<QueueItem>(dataList.size());
        for (Data data : dataList) {
            QueueItem item = new QueueItem(this, nextId(), null);
            if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
                item.setData(data);
            }
            map.put(item.getItemId(), data);
            list.add(item);
        }
        if (store.isEnabled() && !map.isEmpty()) {
            try {
                store.storeAll(map);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        if (!list.isEmpty()) {
            getItemQueue().addAll(list);
            cancelEvictionIfExists();
        }
        return map;
    }

    /**
     * Offers the items to the backup map in bulk. If the memory limit
     * has been achieved the item data will not be kept in-memory.
     * Executed on the backup replica
     *
     * @param dataMap the map from item ID to queue item
     * @see #offerBackup(Data, long)
     */
    public void addAllBackup(Map<Long, Data> dataMap) {
        for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
            QueueItem item = new QueueItem(this, entry.getKey(), null);
            if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
                item.setData(entry.getValue());
            }
            getBackupMap().put(item.getItemId(), item);
        }
    }

    /**
     * Retrieves, but does not remove, the head of this queue, or returns {@code null} if this queue is empty.
     * Loads the data from the queue store if the item data is empty.
     *
     * @return the first item in the queue
     */
    public QueueItem peek() {
        QueueItem item = getItemQueue().peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled() && item.getData() == null) {
            try {
                load(item);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        return item;
    }

    /**
     * Retrieves and removes the head of the queue (in other words, the first item), or returns {@code null} if it is empty.
     * Also calls the queue store for item deletion by item ID.
     *
     * @return the first item in the queue
     */
    public QueueItem poll() {
        QueueItem item = peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled()) {
            try {
                store.delete(item.getItemId());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        getItemQueue().poll();
        age(item, Clock.currentTimeMillis());
        scheduleEvictionIfEmpty();
        return item;
    }

    /**
     * Polls an item on the backup replica. The item ID is predetermined
     * when executing the poll operation on the partition owner.
     * Executed on the backup replica
     *
     * @param itemId the item ID as determined by the primary replica
     */
    public void pollBackup(long itemId) {
        QueueItem item = getBackupMap().remove(itemId);
        if (item != null) {
            // for stats
            age(item, Clock.currentTimeMillis());
        }
    }

    /**
     * Removes items from the queue and the queue store (if configured), up to {@code maxSize} or the size of the queue,
     * whichever is smaller. Also schedules the queue for destruction if it is empty or destroys it immediately if it is
     * empty and {@link QueueConfig#getEmptyQueueTtl()} is 0.
     *
     * @param maxSize the maximum number of items to be removed
     * @return the map of IDs and removed (drained) items
     */
    public Map<Long, Data> drain(int maxSize) {
        int maxSizeParam = maxSize;
        if (maxSizeParam < 0 || maxSizeParam > getItemQueue().size()) {
            maxSizeParam = getItemQueue().size();
        }
        Map<Long, Data> map = createLinkedHashMap(maxSizeParam);
        mapDrainIterator(maxSizeParam, map);
        if (store.isEnabled() && maxSizeParam != 0) {
            try {
                store.deleteAll(map.keySet());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        long current = Clock.currentTimeMillis();
        for (int i = 0; i < maxSizeParam; i++) {
            QueueItem item = getItemQueue().poll();
            // for stats
            age(item, current);
        }
        if (maxSizeParam != 0) {
            scheduleEvictionIfEmpty();
        }
        return map;
    }

    public void mapDrainIterator(int maxSize, Map<Long, Data> map) {
        Iterator<QueueItem> iterator = getItemQueue().iterator();
        for (int i = 0; i < maxSize; i++) {
            QueueItem item = iterator.next();
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }
            map.put(item.getItemId(), item.getData());
        }
    }

    public void drainFromBackup(Set<Long> itemIdSet) {
        for (Long itemId : itemIdSet) {
            pollBackup(itemId);
        }
        dataMap.clear();
    }

    public int size() {
        return Math.min(config.getMaxSize(), getItemQueue().size());
    }

    public int txMapSize() {
        return txMap.size();
    }

    /**
     * Returns the number of queue items contained on this
     * backup replica. A transaction might temporarily reserve
     * a poll operation by removing an item from this map.
     * If the transaction is committed, the map will remain the same.
     * If the transaction is aborted, the item is returned to the map.
     *
     * @return the number of items on this backup replica
     */
    public int backupSize() {
        return getBackupMap().size();
    }

    public Map<Long, Data> clear() {
        long current = Clock.currentTimeMillis();
        Map<Long, Data> map = createLinkedHashMap(getItemQueue().size());
        for (QueueItem item : getItemQueue()) {
            map.put(item.getItemId(), item.getData());
            // for stats
            age(item, current);
        }
        if (store.isEnabled() && !map.isEmpty()) {
            try {
                store.deleteAll(map.keySet());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        getItemQueue().clear();
        dataMap.clear();
        scheduleEvictionIfEmpty();
        return map;
    }

    public void clearBackup(Set<Long> itemIdSet) {
        drainFromBackup(itemIdSet);
    }

    /**
     * iterates all items, checks equality with data
     * This method does not trigger store load.
     */
    public long remove(Data data) {
        Iterator<QueueItem> iterator = getItemQueue().iterator();
        while (iterator.hasNext()) {
            QueueItem item = iterator.next();
            if (data.equals(item.getData())) {
                if (store.isEnabled()) {
                    try {
                        store.delete(item.getItemId());
                    } catch (Exception e) {
                        throw new HazelcastException(e);
                    }
                }
                iterator.remove();
                // for stats
                age(item, Clock.currentTimeMillis());
                scheduleEvictionIfEmpty();
                return item.getItemId();
            }
        }
        return -1;
    }

    /**
     * Removes a queue item from the backup map. This should
     * be executed on the backup replica.
     *
     * @param itemId the queue item ID
     */
    public void removeBackup(long itemId) {
        getBackupMap().remove(itemId);
    }

    /**
     * Checks if the queue contains all items in the dataSet. This method does not trigger store load.
     *
     * @param dataSet the items which should be stored in the queue
     * @return true if the queue contains all items, false otherwise
     */
    public boolean contains(Collection<Data> dataSet) {
        for (Data data : dataSet) {
            boolean contains = false;
            for (QueueItem item : getItemQueue()) {
                if (item.getData() != null && item.getData().equals(data)) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns data in the queue. This method triggers store load.
     *
     * @return the item data in the queue.
     */
    public List<Data> getAsDataList() {
        List<Data> dataList = new ArrayList<Data>(getItemQueue().size());
        for (QueueItem item : getItemQueue()) {
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }
            dataList.add(item.getData());
        }
        return dataList;
    }

    /**
     * Compares if the queue contains the items in the dataList and removes them according to the retain parameter. If
     * the retain parameter is true, it will remove items which are not in the dataList (retaining the items which are in the
     * list). If the retain parameter is false, it will remove items which are in the dataList (retaining all other items which
     * are not in the list).
     * <p>
     * Note: this method will trigger store load.
     *
     * @param dataList the list of items which are to be retained in the queue or which are to be removed from the queue
     * @param retain   does the method retain the items in the list (true) or remove them from the queue (false)
     * @return map of removed items by ID
     */
    public Map<Long, Data> compareAndRemove(Collection<Data> dataList, boolean retain) {
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>();
        for (QueueItem item : getItemQueue()) {
            if (item.getData() == null && store.isEnabled()) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }
            boolean contains = dataList.contains(item.getData());
            if ((retain && !contains) || (!retain && contains)) {
                map.put(item.getItemId(), item.getData());
            }
        }

        mapIterateAndRemove(map);

        return map;
    }

    /**
     * Deletes items from the queue which have IDs contained in the key set of the given map. Also schedules the queue for
     * destruction if it is empty or destroys it immediately if it is empty and {@link QueueConfig#getEmptyQueueTtl()} is 0.
     *
     * @param map the map of items which to be removed.
     */
    public void mapIterateAndRemove(Map<Long, Data> map) {
        if (map.size() <= 0) {
            return;
        }

        if (store.isEnabled()) {
            try {
                store.deleteAll(map.keySet());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        Iterator<QueueItem> iterator = getItemQueue().iterator();
        while (iterator.hasNext()) {
            QueueItem item = iterator.next();
            if (map.containsKey(item.getItemId())) {
                iterator.remove();
                // for stats
                age(item, Clock.currentTimeMillis());
            }
        }
        scheduleEvictionIfEmpty();
    }

    public void compareAndRemoveBackup(Set<Long> itemIdSet) {
        drainFromBackup(itemIdSet);
    }

    /**
     * Tries to load the data for the given queue item. The method will also try to load data in batches if configured to do so.
     * <p>
     * If the {@link QueueStoreWrapper#getBulkLoad()} is 1, it will just load data for the given item. Otherwise, it will
     * load items for other items in the queue too by collecting the IDs of the items in the queue sequentially up to
     * {@link QueueStoreWrapper#getBulkLoad()} items. While doing so, it will make sure that the ID of the initially requested
     * item is also being loaded even though it is not amongst the first {@link QueueStoreWrapper#getBulkLoad()} items in the
     * queue.
     *
     * @param item the item for which the data is being set
     * @throws Exception if there is any exception. For example, when calling methods on the queue store
     */
    private void load(QueueItem item) throws Exception {
        int bulkLoad = store.getBulkLoad();
        bulkLoad = Math.min(getItemQueue().size(), bulkLoad);
        if (bulkLoad == 1) {
            item.setData(store.load(item.getItemId()));
        } else if (bulkLoad > 1) {
            long maxIdToLoad = -1;
            Iterator<QueueItem> iterator = getItemQueue().iterator();
            Set<Long> keySet = createHashSet(bulkLoad);

            keySet.add(item.getItemId());
            while (keySet.size() < bulkLoad && iterator.hasNext()) {
                long itemId = iterator.next().getItemId();
                if (itemId > lastIdLoaded) {
                    keySet.add(itemId);
                    maxIdToLoad = Math.max(itemId, maxIdToLoad);
                }
            }

            Map<Long, Data> values = store.loadAll(keySet);
            lastIdLoaded = maxIdToLoad;
            dataMap.putAll(values);
            item.setData(getDataFromMap(item.getItemId()));
        }
    }

    /**
     * Returns if this queue can accommodate one item.
     *
     * @return if the queue has capacity for one item
     */
    public boolean hasEnoughCapacity() {
        return hasEnoughCapacity(1);
    }

    /**
     * Returns if this queue can accommodate for {@code delta} items.
     *
     * @param delta the number of items that should be stored in the queue
     * @return if the queue has enough capacity for the items
     */
    public boolean hasEnoughCapacity(int delta) {
        return (getItemQueue().size() + delta) <= config.getMaxSize();
    }

    /**
     * Returns the item queue on the partition owner. This method
     * will also move the items from the backup map if this
     * member has been promoted from a backup replica to the
     * partition owner and clear the backup map.
     *
     * @return the item queue
     */
    public Deque<QueueItem> getItemQueue() {
        if (itemQueue == null) {
            itemQueue = new LinkedList<QueueItem>();
            if (backupMap != null && !backupMap.isEmpty()) {
                List<QueueItem> values = new ArrayList<QueueItem>(backupMap.values());
                Collections.sort(values);
                itemQueue.addAll(values);
                QueueItem lastItem = itemQueue.peekLast();
                if (lastItem != null) {
                    setId(lastItem.itemId + ID_PROMOTION_OFFSET);
                }
                backupMap.clear();
                backupMap = null;
            }
        }
        return itemQueue;
    }

    /**
     * Return the map containing queue items when this instance is
     * a backup replica.
     * The map contains both items that are parts of different
     * transactions and items which have already been committed
     * to the queue.
     *
     * @return backup replica map from item ID to queue item
     */
    public Map<Long, QueueItem> getBackupMap() {
        if (backupMap == null) {
            if (itemQueue != null) {
                backupMap = createHashMap(itemQueue.size());
                for (QueueItem item : itemQueue) {
                    backupMap.put(item.getItemId(), item);
                }
                itemQueue.clear();
                itemQueue = null;
            } else {
                backupMap = new HashMap<Long, QueueItem>();
            }
        }
        return backupMap;
    }

    public Data getDataFromMap(long itemId) {
        return dataMap.remove(itemId);
    }

    public void setConfig(QueueConfig config, NodeEngine nodeEngine, QueueService service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
        this.logger = nodeEngine.getLogger(QueueContainer.class);
        this.config = new QueueConfig(config);
        // init QueueStore
        QueueStoreConfig storeConfig = config.getQueueStoreConfig();
        SerializationService serializationService = nodeEngine.getSerializationService();
        ClassLoader classLoader = nodeEngine.getConfigClassLoader();
        this.store = QueueStoreWrapper.create(name, storeConfig, serializationService, classLoader);
    }

    /**
     * Returns the next ID that can be used for uniquely identifying queue items
     */
    private long nextId() {
        return ++idGenerator;
    }

    public long getCurrentId() {
        return idGenerator;
    }

    public QueueWaitNotifyKey getPollWaitNotifyKey() {
        return pollWaitNotifyKey;
    }

    public QueueWaitNotifyKey getOfferWaitNotifyKey() {
        return offerWaitNotifyKey;
    }

    public QueueConfig getConfig() {
        return config;
    }

    private void age(QueueItem item, long currentTime) {
        long elapsed = currentTime - item.getCreationTime();
        if (elapsed <= 0) {
            // elapsed time can not be a negative value, a system clock problem maybe (ignored)
            return;
        }
        totalAgedCount++;
        totalAge += elapsed;

        minAge = Math.min(minAge, elapsed);
        maxAge = Math.max(maxAge, elapsed);
    }

    public void setStats(LocalQueueStatsImpl stats) {
        stats.setMinAge(minAge);
        stats.setMaxAge(maxAge);
        long totalAgedCountVal = Math.max(totalAgedCount, 1);
        stats.setAveAge(totalAge / totalAgedCountVal);
    }

    /**
     * Schedules the queue for destruction if the queue is empty. Destroys the queue immediately the queue is empty and the
     * {@link QueueConfig#getEmptyQueueTtl()} is 0. Upon scheduled execution, the queue will be checked if it is still empty.
     */
    private void scheduleEvictionIfEmpty() {
        int emptyQueueTtl = config.getEmptyQueueTtl();
        if (emptyQueueTtl < 0) {
            return;
        }
        if (getItemQueue().isEmpty() && txMap.isEmpty() && !isEvictionScheduled) {
            if (emptyQueueTtl == 0) {
                nodeEngine.getProxyService().destroyDistributedObject(QueueService.SERVICE_NAME, name);
            } else {
                service.scheduleEviction(name, TimeUnit.SECONDS.toMillis(emptyQueueTtl));
                isEvictionScheduled = true;
            }
        }
    }

    public void cancelEvictionIfExists() {
        if (isEvictionScheduled) {
            service.cancelEviction(name);
            isEvictionScheduled = false;
        }
    }

    public boolean isEvictable() {
        return getItemQueue().isEmpty() && txMap.isEmpty();
    }

    public void rollbackTransaction(String transactionId) {
        Iterator<TxQueueItem> iterator = txMap.values().iterator();

        while (iterator.hasNext()) {
            TxQueueItem item = iterator.next();
            if (transactionId.equals(item.getTransactionId())) {
                iterator.remove();
                if (item.isPollOperation()) {
                    getItemQueue().offerFirst(item);
                    cancelEvictionIfExists();
                }
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(getItemQueue().size());
        for (QueueItem item : getItemQueue()) {
            out.writeObject(item);
        }
        out.writeInt(txMap.size());
        for (TxQueueItem item : txMap.values()) {
            item.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        pollWaitNotifyKey = new QueueWaitNotifyKey(name, "poll");
        offerWaitNotifyKey = new QueueWaitNotifyKey(name, "offer");
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = in.readObject();
            getItemQueue().offer(item);
            setId(item.getItemId());
        }
        int txSize = in.readInt();
        for (int j = 0; j < txSize; j++) {
            TxQueueItem item = new TxQueueItem(this, -1, null);
            item.readData(in);
            txMap.put(item.getItemId(), item);
            setId(item.getItemId());
        }
    }

    public void destroy() {
        if (itemQueue != null) {
            itemQueue.clear();
        }
        if (backupMap != null) {
            backupMap.clear();
        }
        txMap.clear();
        dataMap.clear();
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.QUEUE_CONTAINER;
    }

    void setId(long itemId) {
        idGenerator = Math.max(itemId + 1, idGenerator);
    }
}
