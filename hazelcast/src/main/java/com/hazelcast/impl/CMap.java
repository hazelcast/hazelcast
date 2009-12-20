/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import static com.hazelcast.impl.ClusterOperation.*;

import com.hazelcast.impl.concurrentmap.MultiData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import com.hazelcast.nio.Packet;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Index;
import com.hazelcast.util.SortedHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CMap {

    final static Logger logger = Logger.getLogger(CMap.class.getName());

    final ConcurrentMapManager concurrentMapManager;

    final Node node;

    final int BLOCK_COUNT;

    final Block[] blocks;

    final Address thisAddress;

    final Set<Record> setRemovedRecords = new HashSet<Record>(10000);

    final Set<Record> ownedRecords = new HashSet<Record>(10000);

    final SortedHashMap<Data, Record> mapRecords;

    private final String name;

    final Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);

    final int backupCount;

    final SortedHashMap.OrderingType evictionPolicy;

    final int maxSize;

    final float evictionRate;

    final long ttl; //ttl for entries

    final long maxIdle; //maxIdle for entries

    final Instance.InstanceType instanceType;

    final MapLoader loader;

    final MapStore store;

    final long writeDelayMillis;

    final Set<Record> setDirtyRecords;

    final long removeDelayMillis;

    final long evictionDelayMillis;

    private final Map<Expression, Index<MapEntry>> mapIndexes = new ConcurrentHashMap(10);

    final LocallyOwnedMap locallyOwnedMap;

    final MapNearCache mapNearCache;

    boolean ttlPerRecord = false;

    private volatile Index<MapEntry>[] indexes = null;

    volatile byte[] indexTypes = null;

    long lastEvictionTime = 0;

    public CMap(ConcurrentMapManager concurrentMapManager, String name) {
        this.concurrentMapManager = concurrentMapManager;
        this.BLOCK_COUNT = concurrentMapManager.BLOCK_COUNT;
        this.blocks = concurrentMapManager.blocks;
        this.node = concurrentMapManager.node;
        this.thisAddress = concurrentMapManager.thisAddress;
        this.name = name;
        mapRecords = new SortedHashMap<Data, Record>(10000);
        MapConfig mapConfig = null;
        String mapConfigName = name.substring(2);
        if (mapConfigName.startsWith("__hz_") || mapConfigName.startsWith("l:") || mapConfigName.startsWith("s:")) {
            mapConfig = new MapConfig();
        } else {
            mapConfig = node.getConfig().getMapConfig(mapConfigName);
        }
        this.backupCount = mapConfig.getBackupCount();
        ttl = mapConfig.getTimeToLiveSeconds() * 1000L;
        evictionDelayMillis = mapConfig.getEvictionDelaySeconds() * 1000L;
        maxIdle = mapConfig.getMaxIdleSeconds() * 1000L;
        evictionPolicy = SortedHashMap.getOrderingTypeByName(mapConfig.getEvictionPolicy());
        if (evictionPolicy == SortedHashMap.OrderingType.NONE) {
            maxSize = Integer.MAX_VALUE;
        } else {
            maxSize = (mapConfig.getMaxSize() == 0) ? MapConfig.DEFAULT_MAX_SIZE : mapConfig.getMaxSize();
        }
        evictionRate = mapConfig.getEvictionPercentage() / 100f;
        instanceType = concurrentMapManager.getInstanceType(name);
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        MapStore storeTemp = null;
        MapLoader loaderTemp = null;
        int writeDelaySeconds = -1;
        if (mapStoreConfig != null) {
            if (mapStoreConfig.isEnabled()) {
                String mapStoreClassName = mapStoreConfig.getClassName();
                try {
                    Object storeInstance = Class.forName(mapStoreClassName).newInstance();
                    if (storeInstance instanceof MapLoader) {
                        loaderTemp = (MapLoader) storeInstance;
                    }
                    if (storeInstance instanceof MapStore) {
                        storeTemp = (MapStore) storeInstance;
                    }
                    writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        loader = loaderTemp;
        store = storeTemp;
        writeDelayMillis = (writeDelaySeconds == -1) ? -1L : writeDelaySeconds * 1000L;
        setDirtyRecords = (writeDelayMillis == -1) ? null : new HashSet<Record>(5000);
        if (writeDelaySeconds > 0) {
            removeDelayMillis = concurrentMapManager.GLOBAL_REMOVE_DELAY_MILLIS + writeDelaySeconds;
        } else {
            removeDelayMillis = concurrentMapManager.GLOBAL_REMOVE_DELAY_MILLIS;
        }
        if (evictionPolicy == SortedHashMap.OrderingType.NONE && instanceType == Instance.InstanceType.MAP) {
            locallyOwnedMap = new LocallyOwnedMap();
            concurrentMapManager.mapLocallyOwnedMaps.put(name, locallyOwnedMap);
        } else {
            locallyOwnedMap = null;
        }
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        if (nearCacheConfig == null) {
            mapNearCache = null;
        } else {
            mapNearCache = new MapNearCache(this,
                    SortedHashMap.getOrderingTypeByName(mapConfig.getEvictionPolicy()),
                    nearCacheConfig.getMaxSize(),
                    nearCacheConfig.getTimeToLiveSeconds() * 1000L,
                    nearCacheConfig.getMaxIdleSeconds() * 1000L,
                    nearCacheConfig.isInvalidateOnChange());
            concurrentMapManager.mapCaches.put(name, mapNearCache);
        }
    }

    public void addIndex(Expression expression, boolean ordered) {
        if (!getMapIndexes().containsKey(expression)) {
            Index index = new Index(expression, ordered);
            getMapIndexes().put(expression, index);
            Index[] newIndexes = new Index[getMapIndexes().size()];
            if (getIndexes() != null) {
                System.arraycopy(getIndexes(), 0, newIndexes, 0, getIndexes().length);
            }
            setIndexes(newIndexes);
            getIndexes()[getMapIndexes().size() - 1] = index;
            node.queryService.setIndexes(getName(), getIndexes(), getMapIndexes());
        }
    }

    public Record getRecord(Data key) {
        return mapRecords.get(key);
    }

    public int getBackupCount() {
        return backupCount;
    }

    public void own(Request req) {
        if (req.key == null || req.key.size() == 0) {
            throw new RuntimeException("Key cannot be null " + req.key);
        }
        if (req.value == null) {
            req.value = new Data();
        }
        Record record = toRecord(req);
        markAsActive(record);
        markAsOwned(record);
        updateIndexes(true, req, record);
        record.setVersion(req.version);
    }

    public boolean isMultiMap() {
        return (instanceType == Instance.InstanceType.MULTIMAP);
    }

    public boolean backup(Request req) {
        if (req.key == null || req.key.size() == 0) {
            throw new RuntimeException("Backup key size cannot be 0: " + req.key);
        }
        Record record = getRecord(req.key);
        if (record != null) {
            record.setActive();
            if (req.version > record.getVersion() + 1) {
                Request reqCopy = new Request();
                reqCopy.setFromRequest(req, true);
                req.key = null;
                req.value = null;
                record.addBackupOp(new VersionedBackupOp(this, reqCopy));
                return true;
            } else if (req.version <= record.getVersion()) {
                return false;
            }
        }
        doBackup(req);
        if (record != null) {
            record.setVersion(req.version);
            record.runBackupOps();
        }
        return true;
    }

    public void doBackup(Request req) {
        if (req.key == null || req.key.size() == 0) {
            throw new RuntimeException("Backup key size cannot be zero! " + req.key);
        }
        if (req.operation == CONCURRENT_MAP_BACKUP_PUT) {
            Record rec = toRecord(req);
            if (rec.getVersion() == 0) {
                rec.setVersion(req.version);
            }
            if (req.indexes != null) {
                if (req.indexTypes == null) {
                    throw new RuntimeException("index types cannot be null!");
                }
                if (req.indexes.length != req.indexTypes.length) {
                    throw new RuntimeException("index and type lenghts do not match");
                }
                rec.setIndexes(req.indexes, req.indexTypes);
                if (indexTypes == null) {
                    indexTypes = req.indexTypes;
                } else {
                    if (indexTypes.length != req.indexTypes.length) {
                        throw new RuntimeException("Index types do not match.");
                    }
                }
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE) {
            Record record = getRecord(req.key);
            if (record != null) {
                if (record.getCopyCount() > 0) {
                    record.decrementCopyCount();
                }
                record.setValue(null);
                if (record.isRemovable()) {
                    removeAndPurgeRecord(record);
                }
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_LOCK) {
            Record rec = toRecord(req);
            if (rec.getVersion() == 0) {
                rec.setVersion(req.version);
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_ADD) {
            add(req);
        } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE_MULTI) {
            Record record = getRecord(req.key);
            if (record != null) {
                if (req.value == null) {
                    removeAndPurgeRecord(record);
                } else {
                    if (record.containsValue(req.value)) {
                        if (record.getMultiValues() != null) {
                            Iterator<Data> itValues = record.getMultiValues().iterator();
                            while (itValues.hasNext()) {
                                Data value = itValues.next();
                                if (req.value.equals(value)) {
                                    itValues.remove();
                                }
                            }
                        }
                    }
                }
                if (record.isRemovable()) {
                    removeAndPurgeRecord(record);
                }
            }
        } else {
            logger.log(Level.SEVERE, "Unknown backup operation " + req.operation);
        }
    }

    public int backupSize() {
        int size = 0;
        Collection<Record> records = mapRecords.values();
        for (Record record : records) {
            Block block = blocks[record.getBlockId()];
            if (!thisAddress.equals(block.getOwner())) {
                size += record.valueCount();
            }
        }
        return size;
    }

    public int size() {
        long now = System.currentTimeMillis();
        int size = 0;
        Collection<Record> records = mapRecords.values();
        for (Record record : records) {
            if (record.isActive() && record.isValid(now)) {
                Block block = blocks[record.getBlockId()];
                if (thisAddress.equals(block.getOwner())) {
                    size += record.valueCount();
                }
            }
        }
//        for (int i = 0; i < BLOCK_COUNT; i++) {
//            System.out.println(blocks[i]);
//        }
//        System.out.println(size + " is size.. backup.size " + backupSize() + " ownedEntryCount:" + ownedRecords.size());
//        System.out.println(size + " map size " + mapRecords.size() +  " @ " + node.getThisAddress());
        return size;
    }

    public int valueCount(Data key) {
        long now = System.currentTimeMillis();
        int count = 0;
        Record record = mapRecords.get(key);
        if (record != null && record.isValid(now)) {
            count = record.valueCount();
        }
        return count;
    }

    public boolean contains(Request req) {
        Data key = req.key;
        Data value = req.value;
        if (key != null) {
            Record record = getRecord(req.key);
            if (record == null) {
                return false;
            } else {
                Block block = blocks[record.getBlockId()];
                if (thisAddress.equals(block.getOwner())) {
                    if (record.isActive() && record.isValid()) {
                        touch(record);
                        if (value == null) {
                            return record.valueCount() > 0;
                        } else {
                            return record.containsValue(value);
                        }
                    }
                }
            }
        } else {
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                long now = System.currentTimeMillis();
                if (record.isActive() && record.isValid(now)) {
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
                        if (record.containsValue(value)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public CMapEntry getMapEntry(Request req) {
        Record record = getRecord(req.key);
        if (record == null)
            return null;
        if (!record.isValid()) {
            return null;
        }
        return new CMapEntry(record.getCost(), record.getExpirationTime(), record.getLastAccessTime(), record.getLastUpdateTime(),
                record.getCreationTime(), record.getVersion(), record.getHits(), true);
    }

    public Data get(Request req) {
        Record record = getRecord(req.key);
        if (record == null)
            return null;
        if (!record.isActive()) return null;
        if (!record.isValid()) {
            if (record.isEvictable()) {
                scheduleForEviction(record);
                return null;
            }
        }
        if (req.local && locallyOwnedMap != null) {
            locallyOwnedMap.offerToCache(record);
        }
        record.setLastAccessed();
        touch(record);
        Data data = record.getValue();
        Data returnValue = null;
        if (data != null) {
            returnValue = data;
        } else {
            if (record.getMultiValues() != null) {
                Values values = new Values(record.getMultiValues());
                returnValue = toData(values);
            }
        }
        if (returnValue != null) {
            req.key = null;
        }
        return returnValue;
    }

    public boolean add(Request req) {
        Record record = getRecord(req.key);
        if (record == null) {
            record = createNewRecord(req.key, null);
            req.key = null;
        } else {
            if (record.isActive() && req.operation == CONCURRENT_MAP_ADD_TO_SET) {
                return false;
            }
        }
        record.setActive(true);
        node.queryService.updateIndex(getName(), null, null, record, Integer.MIN_VALUE);
        record.setVersion(record.getVersion() + 1);
        record.incrementCopyCount();
        concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_ADDED, record);
        return true;
    }

    public boolean removeMulti(Request req) {
        Record record = getRecord(req.key);
        if (record == null) return false;
        boolean removed = false;
        if (req.value == null) {
            removed = true;
            markAsRemoved(record);
        } else {
            if (record.containsValue(req.value)) {
                if (record.getMultiValues() != null) {
                    Iterator<Data> itValues = record.getMultiValues().iterator();
                    while (itValues.hasNext()) {
                        Data value = itValues.next();
                        if (req.value.equals(value)) {
                            itValues.remove();
                            removed = true;
                        }
                    }
                }
            }
        }
        if (removed) {
            record.setVersion(record.getVersion() + 1);
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_REMOVED, record.getKey(), req.value, record.getMapListeners());
            logger.log(Level.FINEST, record.getValue() + " RemoveMulti " + record.getMultiValues());
        }
        req.version = record.getVersion();
        return removed;
    }

    public boolean putMulti(Request req) {
        Record record = getRecord(req.key);
        boolean added = true;
        if (record == null) {
            record = createNewRecord(req.key, null);
            req.key = null;
        } else {
            if (!record.isActive()) {
                markAsActive(record);
            }
            if (record.containsValue(req.value)) {
                added = false;
            }
        }
        if (added) {
            node.queryService.updateIndex(getName(), null, null, record, Integer.MIN_VALUE);
            record.addValue(req.value);
            req.value = null;
            record.setVersion(record.getVersion() + 1);
            touch(record);
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_ADDED, record.getKey(), req.value, record.getMapListeners());
        }
        if (req.txnId != -1) {
            concurrentMapManager.unlock(record);
        }
        logger.log(Level.FINEST, record.getValue() + " PutMulti " + record.getMultiValues());
        req.version = record.getVersion();
        return added;
    }

    public void put(Request req) {
        if (ownedRecords.size() >= maxSize) {
            startEviction();
        }
        if (req.value == null) {
            req.value = new Data();
        }
        Record record = getRecord(req.key);
        if (req.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
            if (record != null && record.isActive() && record.isValid() && record.getValue() != null) {
                req.clearForResponse();
                req.response = record.getValue();
                return;
            }
        } else if (req.operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL) {
            if (record == null || !record.isActive() || !record.isValid() || record.getValue() == null) {
                return;
            }
        } else if (req.operation == CONCURRENT_MAP_REPLACE_IF_SAME) {
            if (record == null || !record.isActive() || !record.isValid()) {
                req.response = Boolean.FALSE;
                return;
            }
            MultiData multiData = (MultiData) toObject(req.value);
            if (multiData == null || multiData.size() != 2) {
                throw new RuntimeException("Illegal replaceIfSame argument: " + multiData);
            }
            Data expectedOldValue = multiData.getData(0);
            Data newValue = multiData.getData(1);
            req.value = newValue;
            if (!record.getValue().equals(expectedOldValue)) {
                req.response = Boolean.FALSE;
                return;
            }
        }
        req.value.hash = (int) req.longValue;
        Data oldValue = null;
        boolean created = false;
        if (record == null) {
            record = createNewRecord(req.key, req.value);
            created = true;
        } else {
            markAsActive(record);
            oldValue = record.getValue();
            record.setValue(req.value);
            record.incrementVersion();
            touch(record);
            record.setLastUpdated();
        }
        if (req.ttl > 0) {
            record.setExpirationTime(req.ttl);
            ttlPerRecord = true;
        }
        markAsOwned(record);
        if (oldValue == null) {
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_ADDED, record);
        } else {
            if (mapNearCache != null && mapNearCache.shouldInvalidateOnChange()) {
                sendInvalidation(record);
            }
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_UPDATED, record);
        }
        if (req.txnId != -1) {
            concurrentMapManager.unlock(record);
        }
        updateIndexes(created, req, record);
        markAsDirty(record);
        req.clearForResponse();
        req.version = record.getVersion();
        req.longValue = record.getCopyCount();
        if (req.operation == CONCURRENT_MAP_REPLACE_IF_SAME) {
            req.response = Boolean.TRUE;
        } else {
            req.response = oldValue;
        }
    }

    void sendInvalidation(Record record) {
        for (MemberImpl member : concurrentMapManager.lsMembers) {
            if (!member.localMember()) {
                if (member.getAddress() != null) {
                    Packet packet = concurrentMapManager.obtainPacket();
                    packet.name = getName();
                    packet.key = record.getKey();
                    packet.operation = ClusterOperation.CONCURRENT_MAP_INVALIDATE;
                    boolean sent = concurrentMapManager.send(packet, member.getAddress());
                    if (!sent) {
                        packet.returnToContainer();
                    }
                }
            }
        }
    }

    void startAsyncStoreWrite() {
        logger.log(Level.FINEST, "startAsyncStoreWrite " + setDirtyRecords.size());
        long now = System.currentTimeMillis();
        Iterator<Record> itDirtyRecords = setDirtyRecords.iterator();
        final Map<Data, Data> entriesToStore = new HashMap<Data, Data>();
        final Collection<Data> keysToDelete = new HashSet<Data>();
        while (itDirtyRecords.hasNext()) {
            Record dirtyRecord = itDirtyRecords.next();
            if (dirtyRecord.getWriteTime() > now) {
                if (dirtyRecord.getValue() != null) {
                    entriesToStore.put(dirtyRecord.getKey(), dirtyRecord.getValue());
                } else {
                    keysToDelete.add(dirtyRecord.getKey());
                }
                dirtyRecord.setDirty(false);
                itDirtyRecords.remove();
            }
        }
        final int entriesToStoreSize = entriesToStore.size();
        final int keysToDeleteSize = keysToDelete.size();
        if (entriesToStoreSize > 0 || keysToDeleteSize > 0) {
            concurrentMapManager.executeLocally(new Runnable() {
                public void run() {
                    if (keysToDeleteSize > 0) {
                        if (keysToDeleteSize == 1) {
                            Data key = keysToDelete.iterator().next();
                            store.delete(toObject(key));
                        } else {
                            Collection realKeys = new HashSet();
                            for (Data key : keysToDelete) {
                                realKeys.add(toObject(key));
                            }
                            store.deleteAll(realKeys);
                        }
                    }
                    if (entriesToStoreSize > 0) {
                        Object keyFirst = null;
                        Object valueFirst = null;
                        Set<Map.Entry<Data, Data>> entries = entriesToStore.entrySet();
                        Map realEntries = new HashMap();
                        for (Map.Entry<Data, Data> entry : entries) {
                            Object key = toObject(entry.getKey());
                            Object value = toObject(entry.getValue());
                            realEntries.put(key, value);
                            if (keyFirst == null) {
                                keyFirst = key;
                                valueFirst = value;
                            }
                        }
                        if (entriesToStoreSize == 1) {
                            store.store(keyFirst, valueFirst);
                        } else {
                            store.storeAll(realEntries);
                        }
                    }
                }
            });
        }
    }

    void startRemove() {
        long now = System.currentTimeMillis();
        if (setRemovedRecords.size() > 10) {
            Iterator<Record> itRemovedRecords = setRemovedRecords.iterator();
            while (itRemovedRecords.hasNext()) {
                Record record = itRemovedRecords.next();
                if (record.isActive()) {
                    itRemovedRecords.remove();
                } else if (shouldRemove(record, now)) {
                    itRemovedRecords.remove();
                    removeAndPurgeRecord(record);
                }
            }
        }
    }

    void startEviction() {
        long now = System.currentTimeMillis();
        if ((now - lastEvictionTime) < evictionDelayMillis) {
            return;
        }
        lastEvictionTime = now;
        List<Data> lsKeysToEvict = null;
        if (evictionPolicy == SortedHashMap.OrderingType.NONE) {
            if (ttl != 0 || maxIdle != 0 || ttlPerRecord) {
                Collection<Record> values = mapRecords.values();
                for (Record record : values) {
                    if (record.isActive() && !record.isValid(now)) {
                        if (record.isEvictable()) {
                            if (lsKeysToEvict == null) {
                                lsKeysToEvict = new ArrayList<Data>(100);
                            }
                            markAsRemoved(record);
                            lsKeysToEvict.add(record.getKey());
                        }
                    } else {
                        break;
                    }
                }
            }
        } else {
            Collection<Record> records = mapRecords.values();
            int numberOfRecordsToEvict = (int) (ownedRecords.size() * evictionRate);
            int evictedCount = 0;
            loopRecords:
            for (Record record : records) {
                if (record.isActive() && record.isEvictable()) {
                    if (lsKeysToEvict == null) {
                        lsKeysToEvict = new ArrayList<Data>(numberOfRecordsToEvict);
                    }
                    markAsRemoved(record);
                    lsKeysToEvict.add(record.getKey());
                    if (++evictedCount >= numberOfRecordsToEvict) {
                        break loopRecords;
                    }
                }
            }
        }
        if (lsKeysToEvict != null && lsKeysToEvict.size() > 0) {
            for (final Data key : lsKeysToEvict) {
                concurrentMapManager.evictAsync(getName(), key);
            }
        }
    }

    final boolean evict(Request req) {
        Record record = getRecord(req.key);
        if (record != null && record.isEvictable()) {
            if (ownerForSure(record)) {
                if (req.operation == CONCURRENT_MAP_EVICT_INTERNAL && record.isActive()) {
                    return false;
                }
                concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_EVICTED, record.getKey(), record.getValue(), record.getMapListeners());
                removeAndPurgeRecord(record);
                return true;
            }
        }
        return false;
    }

    final boolean ownerForSure(Record record) {
        Block block = blocks[record.getBlockId()];
        return block != null && !block.isMigrating() && thisAddress.equals(block.getOwner());
    }

    public Record toRecord(Request req) {
        Record record = getRecord(req.key);
        if (record == null) {
            if (isMultiMap()) {
                record = createNewRecord(req.key, null);
                record.addValue(req.value);
            } else {
                record = createNewRecord(req.key, req.value);
            }
            req.key = null;
        } else {
            if (req.value != null) {
                if (isMultiMap()) {
                    record.addValue(req.value);
                } else {
                    record.setValue(req.value);
                }
            }
        }
        req.value = null;
        record.setCopyCount((int) req.longValue);
        if (req.lockCount >= 0) {
            record.setLockAddress(req.lockAddress);
            record.setLockThreadId(req.lockThreadId);
            record.setLockCount(req.lockCount);
        }
        return record;
    }

    public boolean removeItem(Request req) {
        Record record = mapRecords.get(req.key);
        req.key = null;
        if (record == null) {
            return false;
        }
        //The record set as removable, also it is not "really" removed yet. It should be considered as removed
        if (record.isRemovable()) {
            return false;
        }
        if (req.txnId != -1) {
            concurrentMapManager.unlock(record);
        }
        boolean removed = false;
        if (record.getCopyCount() > 0) {
            record.decrementCopyCount();
            removed = true;
        } else if (record.getValue() != null) {
            removed = true;
        } else if (record.getMultiValues() != null) {
            removed = true;
        }
        if (removed) {
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_REMOVED, record);
            record.setVersion(record.getVersion() + 1);
            if (record.getValue() != null) {
                record.setValue(null);
            } else if (record.getMultiValues() != null) {
                record.setMultiValues(null);
            }
        }
        req.version = record.getVersion();
        req.longValue = record.getCopyCount();
        if (record.isRemovable()) {
            markAsRemoved(record);
        }
        return true;
    }

    public void remove(Request req) {
        Record record = mapRecords.get(req.key);
        if (record == null) {
            req.clearForResponse();
            return;
        }
        if (req.txnId != -1) {
            concurrentMapManager.unlock(record);
        }
        if (!record.isValid()) {
            if (record.isEvictable()) {
                scheduleForEviction(record);
                return;
            }
        }
        if (req.value != null) {
            if (record.getValue() != null) {
                if (!record.getValue().equals(req.value)) {
                    return;
                }
            }
        }
        Data oldValue = record.getValue();
        if (oldValue == null && record.getMultiValues() != null && record.getMultiValues().size() > 0) {
            Values values = new Values(record.getMultiValues());
            oldValue = toData(values);
            record.setMultiValues(null);
        }
        if (oldValue != null) {
            if (mapNearCache != null && mapNearCache.shouldInvalidateOnChange()) {
                sendInvalidation(record);
            }
            concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_REMOVED, record.getKey(), oldValue, record.getMapListeners());
            record.incrementVersion();
            record.setValue(null);
            record.setMultiValues(null);
        }
        if (req.txnId != -1) {
            concurrentMapManager.unlock(record);
        }
        if (record.isRemovable()) {
            markAsRemoved(record);
        }
        req.clearForResponse();
        req.version = record.getVersion();
        req.longValue = record.getCopyCount();
        if (oldValue != null) {
            req.key = null;
        }
        req.response = oldValue;
    }

    void reset() {
        if (locallyOwnedMap != null) {
            locallyOwnedMap.reset();
        }
        mapRecords.clear();
        ownedRecords.clear();
        if (setDirtyRecords != null) {
            setDirtyRecords.clear();
        }
        setRemovedRecords.clear();
        node.queryService.reset(getName());
    }

    void scheduleForEviction(Record record) {
        SortedHashMap.moveToTop(mapRecords, record.getKey());
    }

    void touch(Record record) {
        record.setLastTouchTime(System.currentTimeMillis());
        if (evictionPolicy == SortedHashMap.OrderingType.NONE) return;
        SortedHashMap.touch(mapRecords, record.getKey(), evictionPolicy);
    }

    void markAsDirty(Record record) {
        if (!record.isDirty()) {
            record.setDirty(true);
            if (writeDelayMillis > 0) {
                record.setWriteTime(System.currentTimeMillis() + writeDelayMillis);
                setDirtyRecords.add(record);
            }
        }
    }

    void markAsActive(Record record) {
        if (!record.isActive()) {
            record.setActive();
            setRemovedRecords.remove(record);
            record.setCreationTime(System.currentTimeMillis());
        }
        markAsOwned(record);
    }

    boolean shouldRemove(Record record, long now) {
        return !record.isActive() && ((now - record.getRemoveTime()) > removeDelayMillis);
    }

    void markAsRemoved(Record record) {
        if (record.isActive()) {
            record.markRemoved();
            setRemovedRecords.add(record);
        }
        ownedRecords.remove(record);
    }

    void removeAndPurgeRecord(Record record) {
        Block ownerBlock = blocks[record.getBlockId()];
        if (thisAddress.equals(ownerBlock.getRealOwner())) {
            node.queryService.updateIndex(getName(), null, null, record, Integer.MIN_VALUE);
        }
        mapRecords.remove(record.getKey());
    }

    void markAsOwned(Record record) {
        ownedRecords.add(record);
    }

    void updateIndexes(boolean created, Request request, Record record) {
        int valueHash = (record.getValue() != null) ? record.getValue().hashCode() : Integer.MIN_VALUE;
//        if (true) return;
        if (request.indexes != null) {
            int indexCount = request.indexes.length;
            if (indexCount == 0)
                throw new RuntimeException(node.getName() + " request countains no index " + request.indexes);
            if (getMapIndexes().size() > indexCount) {
                throw new RuntimeException(node.getName() + ": indexCount=" + indexCount + " but expected " + getMapIndexes().size());
            }
            long[] newIndexes = request.indexes;
            request.indexes = null;
            byte[] indexTypes = request.indexTypes;
            request.indexTypes = null;
            if (newIndexes.length != indexTypes.length) {
                throw new RuntimeException();
            }
            node.queryService.updateIndex(getName(), newIndexes, indexTypes, record, valueHash);
        } else if (created || record.getValueHash() != valueHash) {
            node.queryService.updateIndex(getName(), null, null, record, valueHash);
        }
    }

    Record createNewRecord(Data key, Data value) {
        if (key == null || key.size() == 0) {
            throw new RuntimeException("Cannot create record from a 0 size key: " + key);
        }
        int blockId = concurrentMapManager.getBlockId(key);
        Record rec = new Record(node.factory, getName(), blockId, key, value, ttl, maxIdle, concurrentMapManager.newRecordId());
        Block ownerBlock = concurrentMapManager.getOrCreateBlock(blockId);
        if (thisAddress.equals(ownerBlock.getRealOwner())) {
            ownedRecords.add(rec);
        }
        mapRecords.put(key, rec);
        if (evictionPolicy != SortedHashMap.OrderingType.NONE) {
            if (maxSize != Integer.MAX_VALUE) {
                int limitSize = (maxSize / concurrentMapManager.lsMembers.size());
                if (ownedRecords.size() > limitSize) {
                    startEviction();
                }
            }
        }
        return rec;
    }

    public void addListener(Data key, Address address, boolean includeValue) {
        if (key == null || key.size() == 0) {
            mapListeners.put(address, includeValue);
        } else {
            Record rec = getRecord(key);
            if (rec == null) {
                rec = createNewRecord(key, null);
            }
            rec.addListener(address, includeValue);
        }
    }

    public void removeListener(Data key, Address address) {
        if (key == null || key.size() == 0) {
            mapListeners.remove(address);
        } else {
            Record rec = getRecord(key);
            if (rec != null) {
                rec.removeListener(address);
            }
        }
    }

    public static class Values implements Collection, DataSerializable {
        List<Data> lsValues = null;

        public Values() {
        }

        public Values(List<Data> lsValues) {
            super();
            this.lsValues = lsValues;
        }

        public boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public boolean contains(Object o) {
            if (o == null) {
                throw new IllegalArgumentException("Contains cannot have null argument.");
            }
            Iterator it = iterator();
            while (it.hasNext()) {
                Object v = it.next();
                if (o.equals(v)) {
                    return true;
                }
            }
            return false;
        }

        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return (size() == 0);
        }

        public Iterator iterator() {
            return new ValueIterator(lsValues.iterator());
        }

        class ValueIterator implements Iterator {
            final Iterator<Data> it;

            public ValueIterator(Iterator<Data> it) {
                super();
                this.it = it;
            }

            public boolean hasNext() {
                return it.hasNext();
            }

            public Object next() {
                Data value = it.next();
                return toObject(value);
            }

            public void remove() {
                it.remove();
            }
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public int size() {
            return (lsValues == null) ? 0 : lsValues.size();
        }

        public Object[] toArray() {
            if (size() == 0) {
                return null;
            }
            return toArray(new Object[size()]);
        }

        public Object[] toArray(Object[] a) {
            int size = size();
            if (size == 0) {
                return null;
            }
            if (a == null || a.length < size) {
                a = new Object[size];
            }
            for (int i = 0; i < size; i++) {
                a[i] = toObject(lsValues.get(i));
            }
            return a;
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            lsValues = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                lsValues.add(data);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(lsValues.size());
            for (Data data : lsValues) {
                data.writeData(out);
            }
        }
    }

    public static class CMapEntry implements HazelcastInstanceAware, MapEntry, DataSerializable {
        private long cost = 0;
        private long expirationTime = 0;
        private long lastAccessTime = 0;
        private long lastUpdateTime = 0;
        private long creationTime = 0;
        private long version = 0;
        private int hits = 0;
        private boolean valid = true;
        private String name = null;
        private Object key = null;
        private Object value = null;
        private HazelcastInstance hazelcastInstance = null;

        public CMapEntry() {
        }

        public CMapEntry(long cost, long expirationTime, long lastAccessTime, long lastUpdateTime, long creationTime, long version, int hits, boolean valid) {
            this.cost = cost;
            this.expirationTime = expirationTime;
            this.lastAccessTime = lastAccessTime;
            this.lastUpdateTime = lastUpdateTime;
            this.creationTime = creationTime;
            this.version = version;
            this.hits = hits;
            this.valid = valid;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeLong(cost);
            out.writeLong(expirationTime);
            out.writeLong(lastAccessTime);
            out.writeLong(lastUpdateTime);
            out.writeLong(creationTime);
            out.writeLong(version);
            out.writeInt(hits);
            out.writeBoolean(valid);
        }

        public void readData(DataInput in) throws IOException {
            cost = in.readLong();
            expirationTime = in.readLong();
            lastAccessTime = in.readLong();
            lastUpdateTime = in.readLong();
            creationTime = in.readLong();
            version = in.readLong();
            hits = in.readInt();
            valid = in.readBoolean();
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public void set(String name, Object key) {
            this.name = name;
            this.key = key;
        }

        public long getCost() {
            return cost;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public int getHits() {
            return hits;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public long getVersion() {
            return version;
        }

        public boolean isValid() {
            return valid;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            if (value == null) {
                FactoryImpl factory = (FactoryImpl) hazelcastInstance;
                value = ((FactoryImpl.MProxy) factory.getOrCreateProxyByName(name)).get(key);
            }
            return value;
        }

        public Object setValue(Object value) {
            Object oldValue = this.value;
            FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            ((FactoryImpl.MProxy) factory.getOrCreateProxyByName(name)).put(key, value);
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CMapEntry cMapEntry = (CMapEntry) o;
            return !(key != null ? !key.equals(cMapEntry.key) : cMapEntry.key != null) &&
                    !(name != null ? !name.equals(cMapEntry.name) : cMapEntry.name != null);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("MapEntry");
            sb.append("{key=").append(key);
            sb.append(", valid=").append(valid);
            sb.append(", hits=").append(hits);
            sb.append(", version=").append(version);
            sb.append(", creationTime=").append(creationTime);
            sb.append(", lastUpdateTime=").append(lastUpdateTime);
            sb.append(", lastAccessTime=").append(lastAccessTime);
            sb.append(", expirationTime=").append(expirationTime);
            sb.append(", cost=").append(cost);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        return "CMap [" + getName() + "] size=" + size() + ", backup-size=" + backupSize();
    }

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the mapIndexes
	 */
	public Map<Expression, Index<MapEntry>> getMapIndexes() {
		return mapIndexes;
	}

	/**
	 * @param indexes the indexes to set
	 */
	public void setIndexes(Index<MapEntry>[] indexes) {
		this.indexes = indexes;
	}

	/**
	 * @return the indexes
	 */
	public Index<MapEntry>[] getIndexes() {
		return indexes;
	}
}
