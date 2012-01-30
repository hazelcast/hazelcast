/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.impl.base.DataRecordEntry;
import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.impl.base.Values;
import com.hazelcast.impl.concurrentmap.*;
import com.hazelcast.impl.monitor.LocalMapStatsImpl;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.merge.MergePolicy;
import com.hazelcast.nio.*;
import com.hazelcast.query.Expression;
import com.hazelcast.query.MapIndexService;
import com.hazelcast.query.Predicates;
import com.hazelcast.util.ConcurrentHashSet;
import com.hazelcast.util.SortedHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.core.Prefix.*;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.nio.IOUtil.toData;

public class CMap {

    private static final Comparator<MapEntry> LRU_COMPARATOR = new LRUMapEntryComparator();
    private static final Comparator<MapEntry> LFU_COMPARATOR = new LFUMapEntryComparator();

    enum EvictionPolicy {
        LRU,
        LFU,
        NONE
    }

    enum InitializationState {
        NONE,
        INITIALIZING,
        INITIALIZED;

        boolean notInitialized() {
            return NONE == this;
        }
    }

    final ILogger logger;

    final ConcurrentMapManager concurrentMapManager;

    final Node node;

    final int PARTITION_COUNT;

    final Address thisAddress;

    final ConcurrentMap<Data, Record> mapRecords = new ConcurrentHashMap<Data, Record>(10000, 0.75f, 1);

    final String name;

    final MapConfig mapConfig;

    final MultiMapConfig multiMapConfig;

    final Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);

    int backupCount;

    EvictionPolicy evictionPolicy;

    Comparator<MapEntry> evictionComparator;

    MapMaxSizePolicy maxSizePolicy;

    float evictionRate;

    long ttl; //ttl for entries

    long maxIdle; //maxIdle for entries

    final Instance.InstanceType instanceType;

    final MapLoader loader;

    final MapStore store;

    private MapStoreWrapper mapStoreWrapper;

    final MergePolicy mergePolicy;

    final long writeDelayMillis;

    final long removeDelayMillis;

    final MapIndexService mapIndexService;

    final NearCache nearCache;

    final long creationTime;

    final boolean multiMapSet;

    boolean readBackupData;

    boolean cacheValue;

    final boolean mapForQueue;

    volatile boolean ttlPerRecord = false;

    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long lastEvictionTime = 0;

    DistributedLock lockEntireMap = null;

    volatile InitializationState initState = InitializationState.NONE;

    final Object initLock = new Object();

    final LocalUpdateListener localUpdateListener;

    final MergePolicy wanMergePolicy;

    final ConcurrentMap<Data, LocalLock> mapLocalLocks = new ConcurrentHashMap<Data, LocalLock>(10000);

    final AtomicBoolean cleanupActive = new AtomicBoolean(false);

    CMap(ConcurrentMapManager concurrentMapManager, String name) {
        this.concurrentMapManager = concurrentMapManager;
        this.logger = concurrentMapManager.node.getLogger(CMap.class.getName());
        this.PARTITION_COUNT = concurrentMapManager.PARTITION_COUNT;
        this.node = concurrentMapManager.node;
        this.thisAddress = concurrentMapManager.thisAddress;
        this.name = name;
        mapForQueue = name.startsWith(MAP_FOR_QUEUE);
        instanceType = ConcurrentMapManager.getInstanceType(name);
        String mapConfigName = name.substring(2);
        if (isMultiMap()
                || mapConfigName.startsWith(HAZELCAST)
                || mapConfigName.startsWith(AS_LIST)
                || mapConfigName.startsWith(AS_SET)) {
            mapConfig = new MapConfig();
        } else if (mapForQueue) {
            String queueShortName = name.substring(4);
            QueueConfig qConfig = node.getConfig().findMatchingQueueConfig(queueShortName);
            mapConfig = node.getConfig().findMatchingMapConfig(qConfig.getBackingMapRef());
        } else {
            mapConfig = node.getConfig().findMatchingMapConfig(mapConfigName);
        }
        this.mapIndexService = new MapIndexService(mapConfig.isValueIndexed());
        setRuntimeConfig(mapConfig);
        if (mapForQueue || node.groupProperties.ELASTIC_MEMORY_ENABLED.getBoolean()) {
            cacheValue = false;
        }
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        int writeDelaySeconds = -1;
        if (!node.isLiteMember() && mapStoreConfig != null) {
            try {
                MapStoreFactory factory = (MapStoreFactory) mapStoreConfig.getFactoryImplementation();
                if (factory == null) {
                    String factoryClassName = mapStoreConfig.getFactoryClassName();
                    if (factoryClassName != null && !"".equals(factoryClassName)) {
                        factory = (MapStoreFactory)
                                Serializer.loadClass(node.getConfig().getClassLoader(), factoryClassName).newInstance();
                    }
                }
                Object storeInstance = factory == null ? mapStoreConfig.getImplementation() :
                        factory.newMapStore(name, mapStoreConfig.getProperties());
                if (storeInstance == null) {
                    String mapStoreClassName = mapStoreConfig.getClassName();
                    storeInstance = Serializer.loadClass(node.getConfig().getClassLoader(), mapStoreClassName).newInstance();
                }
                mapStoreWrapper = new MapStoreWrapper(storeInstance,
                        node.factory.getHazelcastInstanceProxy(),
                        mapStoreConfig.getProperties(),
                        mapConfigName, mapStoreConfig.isEnabled());
                if (!mapStoreWrapper.isMapLoader() && !mapStoreWrapper.isMapStore()) {
                    throw new Exception("MapStore class [" + storeInstance.getClass().getName()
                            + "] should implement either MapLoader or MapStore!");
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
            writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
        }
        writeDelayMillis = (writeDelaySeconds == -1) ? -1L : writeDelaySeconds * 1000L;
        if (writeDelaySeconds > 0) {
            removeDelayMillis = concurrentMapManager.GLOBAL_REMOVE_DELAY_MILLIS + writeDelayMillis;
        } else {
            removeDelayMillis = concurrentMapManager.GLOBAL_REMOVE_DELAY_MILLIS;
        }
        loader = (mapStoreWrapper == null || !mapStoreWrapper.isMapLoader()) ? null : mapStoreWrapper;
        store = (mapStoreWrapper == null || !mapStoreWrapper.isMapStore()) ? null : mapStoreWrapper;
        NearCacheConfig nearCacheConfig = mapConfig.getNearCacheConfig();
        if (nearCacheConfig == null) {
            nearCache = null;
        } else {
            NearCache nearCache = new NearCache(this,
                    SortedHashMap.getOrderingTypeByName(nearCacheConfig.getEvictionPolicy()),
                    nearCacheConfig.getMaxSize(),
                    nearCacheConfig.getTimeToLiveSeconds() * 1000L,
                    nearCacheConfig.getMaxIdleSeconds() * 1000L,
                    nearCacheConfig.isInvalidateOnChange());
            final NearCache anotherNearCache = concurrentMapManager.mapCaches.putIfAbsent(name, nearCache);
            if (anotherNearCache != null) {
                nearCache = anotherNearCache;
            }
            this.nearCache = nearCache;
        }
        this.mergePolicy = getMergePolicy(mapConfig.getMergePolicy());
        this.creationTime = System.currentTimeMillis();
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef != null) {
            this.localUpdateListener = node.wanReplicationService.getWanReplication(wanReplicationRef.getName());
            this.wanMergePolicy = getMergePolicy(wanReplicationRef.getMergePolicy());
        } else {
            this.localUpdateListener = null;
            this.wanMergePolicy = null;
        }
        if (instanceType.isMultiMap()) {
            String shortMultiMapName = name.substring(4);
            multiMapConfig = node.getConfig().getMultiMapConfig(shortMultiMapName);
            if (multiMapConfig.getValueCollectionType() == MultiMapConfig.ValueCollectionType.SET) {
                multiMapSet = true;
            } else {
                multiMapSet = false;
            }
        } else {
            multiMapConfig = null;
            multiMapSet = false;
        }
        if (!mapForQueue) {
            initializeIndexes();
            initializeListeners();
        }
    }

    private void initializeIndexes() {
        for (MapIndexConfig index : mapConfig.getMapIndexConfigs()) {
            if (index.getAttribute() != null) {
                addIndex(Predicates.get(index.getAttribute()), index.isOrdered(), -1);
            } else if (index.getExpression() != null) {
                addIndex(index.getExpression(), index.isOrdered(), -1);
            }
        }
    }

    private void initializeListeners() {
        List<EntryListenerConfig> listenerConfigs = null;
        if (isMultiMap()) {
            listenerConfigs = multiMapConfig.getEntryListenerConfigs();
        } else {
            listenerConfigs = mapConfig.getEntryListenerConfigs();
        }
        if (listenerConfigs != null && !listenerConfigs.isEmpty()) {
            for (EntryListenerConfig lc : listenerConfigs) {
                try {
                    node.listenerManager.createAndAddListenerItem(name, lc, instanceType);
                    if (lc.isLocal()) {
                        addListener(null, thisAddress, lc.isIncludeValue());
                    } else {
                        for (MemberImpl member : node.clusterManager.getMembers()) {
                            addListener(null, member.getAddress(), lc.isIncludeValue());
                        }
                    }
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }

    MergePolicy getMergePolicy(String mergePolicyName) {
        MergePolicy mergePolicyTemp = null;
        if (mergePolicyName != null && !"hz.NO_MERGE".equalsIgnoreCase(mergePolicyName)) {
            MergePolicyConfig mergePolicyConfig = node.getConfig().getMergePolicyConfig(mergePolicyName);
            if (mergePolicyConfig != null) {
                mergePolicyTemp = mergePolicyConfig.getImplementation();
                if (mergePolicyTemp == null) {
                    String mergeClassName = mergePolicyConfig.getClassName();
                    try {
                        mergePolicyTemp = (MergePolicy) Serializer.loadClass(node.getConfig().getClassLoader(), mergeClassName).newInstance();
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            }
        }
        return mergePolicyTemp;
    }

    public void setRuntimeConfig(MapConfig mapConfig) {
        backupCount = mapConfig.getBackupCount();
        ttl = mapConfig.getTimeToLiveSeconds() * 1000L;
        maxIdle = mapConfig.getMaxIdleSeconds() * 1000L;
        evictionPolicy = mapConfig.getEvictionPolicy() != null
                ? EvictionPolicy.valueOf(mapConfig.getEvictionPolicy())
                : EvictionPolicy.NONE;
        readBackupData = mapConfig.isReadBackupData();
        cacheValue = mapConfig.isCacheValue();
        MaxSizeConfig maxSizeConfig = mapConfig.getMaxSizeConfig();
        if (MaxSizeConfig.POLICY_MAP_SIZE_PER_JVM.equals(maxSizeConfig.getMaxSizePolicy())) {
            maxSizePolicy = new MaxSizePerJVMPolicy(maxSizeConfig);
        } else if (MaxSizeConfig.POLICY_CLUSTER_WIDE_MAP_SIZE.equals(maxSizeConfig.getMaxSizePolicy())) {
            maxSizePolicy = new MaxSizeClusterWidePolicy(maxSizeConfig);
        } else if (MaxSizeConfig.POLICY_PARTITIONS_WIDE_MAP_SIZE.equals(maxSizeConfig.getMaxSizePolicy())) {
            maxSizePolicy = new MaxSizePartitionsWidePolicy(maxSizeConfig);
        } else if (MaxSizeConfig.POLICY_USED_HEAP_SIZE.equals(maxSizeConfig.getMaxSizePolicy())) {
            maxSizePolicy = new MaxSizeHeapPolicy(maxSizeConfig);
        } else if (MaxSizeConfig.POLICY_USED_HEAP_PERCENTAGE.equals(maxSizeConfig.getMaxSizePolicy())) {
            maxSizePolicy = new MaxSizeHeapPercentagePolicy(maxSizeConfig);
        } else {
            maxSizePolicy = null;
        }
        if (evictionPolicy == EvictionPolicy.NONE) {
            evictionComparator = null;
        } else {
            if (evictionPolicy == EvictionPolicy.LRU) {
                evictionComparator = new ComparatorWrapper(LRU_COMPARATOR);
            } else {
                evictionComparator = new ComparatorWrapper(LFU_COMPARATOR);
            }
        }
        evictionRate = mapConfig.getEvictionPercentage() / 100f;
    }

    public MapConfig getRuntimeConfig() {
        MapConfig mapConfig = new MapConfig(name);
        mapConfig.setBackupCount(backupCount);
        mapConfig.setTimeToLiveSeconds((int) (ttl / 1000));
        mapConfig.setMaxIdleSeconds((int) (maxIdle / 1000));
        mapConfig.setEvictionPolicy(evictionPolicy.toString());
        mapConfig.setReadBackupData(readBackupData);
        mapConfig.setCacheValue(cacheValue);
        if (maxSizePolicy != null) {
            mapConfig.getMaxSizeConfig().setMaxSizePolicy(maxSizePolicy.getMaxSizeConfig().getMaxSizePolicy());
            mapConfig.getMaxSizeConfig().setSize(maxSizePolicy.getMaxSizeConfig().getSize());
        }
        mapConfig.setEvictionPercentage((int) (evictionRate * 100));
        return mapConfig;
    }

    public Object getInitLock() {
        return initLock;
    }

    boolean isUserMap() {
        return !name.startsWith(MAP_HAZELCAST);
    }

    final boolean isNotLocked(Request request) {
        return (lockEntireMap == null
                || !lockEntireMap.isLocked()
                || lockEntireMap.isLockedBy(request.lockAddress, request.lockThreadId));
    }

    final boolean overCapacity(Request request) {
        if (maxSizePolicy.overCapacity()) {
            boolean addOp = (request.operation == ClusterOperation.CONCURRENT_MAP_PUT)
                    || (request.operation == ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT);
            if (addOp) {
                Record record = getRecord(request);
                if (record == null) {
                    concurrentMapManager.executeCleanup(CMap.this, true);
                    return true;
                }
            }
        }
        return false;
    }

    public void lockMap(Request request) {
        if (request.operation == CONCURRENT_MAP_LOCK_MAP) {
            if (lockEntireMap == null) {
                lockEntireMap = new DistributedLock();
            }
            if (!lockEntireMap.isLockedBy(request.lockAddress, request.lockThreadId)) {
                lockEntireMap.lock(request.lockAddress, request.lockThreadId);
            }
            request.clearForResponse();
            request.response = Boolean.TRUE;
        } else if (request.operation == CONCURRENT_MAP_UNLOCK_MAP) {
            if (lockEntireMap != null) {
                request.response = lockEntireMap.unlock(request.lockAddress, request.lockThreadId);
            } else {
                request.response = Boolean.TRUE;
            }
        }
    }

    public void addIndex(Expression expression, boolean ordered, int attributeIndex) {
        mapIndexService.addIndex(expression, ordered, attributeIndex);
    }

    public Record getRecord(Data key) {
        return mapRecords.get(key);
    }

    public int getBackupCount() {
        return backupCount;
    }

    public void own(DataRecordEntry dataRecordEntry) {
        Record record = storeDataRecordEntry(dataRecordEntry);
        if (record != null) {
            if (!isMultiMap() && record.getValueData() != null) {
                updateIndexes(record);
            }
        }
    }

    public void storeAsBackup(DataRecordEntry dataRecordEntry) {
        storeDataRecordEntry(dataRecordEntry);
    }

    public Record storeDataRecordEntry(DataRecordEntry dataRecordEntry) {
        Record record;
        if (isMultiMap()) {
            record = getRecord(dataRecordEntry.getKeyData());
            if (record == null) {
                record = createNewRecord(dataRecordEntry.getKeyData(), null);
                mapRecords.put(dataRecordEntry.getKeyData(), record);
            }
            if (record.getMultiValues() == null) {
                record.setMultiValues(createMultiValuesCollection());
            }
            record.getMultiValues().add(new ValueHolder(dataRecordEntry.getValueData()));
        } else {
            Record existing = getRecord(dataRecordEntry.getKeyData());
            if (existing != null) {
                mapIndexService.remove(existing);
            }
            record = createNewRecord(dataRecordEntry.getKeyData(), dataRecordEntry.getValueData());
            record.setCreationTime(dataRecordEntry.getCreationTime());
            record.setExpirationTime(dataRecordEntry.getExpirationTime() - System.currentTimeMillis());
            record.setMaxIdle(dataRecordEntry.getRemainingIdle());
            record.setIndexes(dataRecordEntry.getIndexes(), dataRecordEntry.getIndexTypes());
            if (dataRecordEntry.getLockAddress() != null && dataRecordEntry.getLockThreadId() != -1) {
                record.lock(dataRecordEntry.getLockThreadId(), dataRecordEntry.getLockAddress());
            }
            mapRecords.put(dataRecordEntry.getKeyData(), record);
        }
        record.setVersion(dataRecordEntry.getVersion());
        markAsActive(record);
        return record;
    }

    public void own(Request req) {
        if (req.key == null || req.key.size() == 0) {
            throw new RuntimeException("Key cannot be null " + req.key);
        }
        if (req.value == null) {
            if (isSet() || isList()) {
                req.value = new Data();
            }
        }
        Record record = toRecord(req);
        if (req.ttl <= 0 || req.timeout <= 0) {
            record.setInvalid();
        } else {
            record.setExpirationTime(req.ttl);
            record.setMaxIdle(req.timeout);
        }
        markAsActive(record);
        if (store != null && writeDelayMillis > 0) {
            markAsDirty(record);
        }
        if (req.value != null) {
            updateIndexes(record);
        }
        record.setVersion(req.version);
    }

    public boolean isMultiMap() {
        return (instanceType == Instance.InstanceType.MULTIMAP);
    }

    public boolean isSet() {
        return (instanceType == Instance.InstanceType.SET);
    }

    public boolean isList() {
        return (instanceType == Instance.InstanceType.LIST);
    }

    public boolean isMap() {
        return (instanceType == Instance.InstanceType.MAP);
    }

    public boolean backup(Request req) {
        if (req.key == null || req.key.size() == 0) {
            throw new RuntimeException("Backup key size cannot be 0: " + req.key);
        }
        if (isMap() || isSet()) {
            return backupOneValue(req);
        } else {
            return backupMultiValue(req);
        }
    }

    /**
     * Map and Set have one value only so we can ignore the
     * values with old version.
     *
     * @param req
     * @return
     */
    private boolean backupOneValue(Request req) {
        Record record = getRecord(req);
        if (record != null && record.isActive() && req.version < record.getVersion()) {
            return false;
        }
        doBackup(req);
        if (record != null) {
            record.setVersion(req.version);
        }
        return true;
    }

    /**
     * MultiMap and List have to use versioned backup
     * because each key can have multiple values and
     * we don't want to miss backing up each one.
     *
     * @param req
     * @return
     */
    private boolean backupMultiValue(Request req) {
        Record record = getRecord(req);
        if (record != null) {
            record.setActive();
            if (req.version > record.getVersion() + 1) {
                Request reqCopy = req.hardCopy();
                record.addBackupOp(new VersionedBackupOp(this, reqCopy));
                return true;
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
            Record record = toRecord(req);
            markAsActive(record);
            record.setVersion(req.version);
            if (req.indexes != null) {
                if (req.indexTypes == null) {
                    throw new RuntimeException("index types cannot be null!");
                }
                if (req.indexes.length != req.indexTypes.length) {
                    throw new RuntimeException("index and type lengths do not match");
                }
                record.setIndexes(req.indexes, req.indexTypes);
            }
            if (req.ttl > 0 && req.ttl < Long.MAX_VALUE) {
                record.setExpirationTime(req.ttl);
                ttlPerRecord = true;
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE) {
            Record record = getRecord(req);
            if (record != null) {
                if (record.isActive()) {
                    markAsEvicted(record);
                }
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_LOCK) {
            Record rec = toRecord(req);
            if (rec.getVersion() == 0) {
                rec.setVersion(req.version);
            }
            if (rec.getLockCount() == 0 && rec.valueCount() == 0) {
                markAsEvicted(rec);
            }
        } else if (req.operation == CONCURRENT_MAP_BACKUP_ADD) {
            add(req, true);
        } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE_MULTI) {
            Record record = getRecord(req);
            if (record != null) {
                if (req.value == null) {
                    markAsEvicted(record);
                } else {
                    // FIXME: This should be done out of service thread
                    // if (record.containsValue(req.value)) {
                    Collection<ValueHolder> multiValues = record.getMultiValues();
                    if (multiValues != null) {
                        multiValues.remove(new ValueHolder(req.value));
                    }
                    // }
                    if (record.valueCount() == 0) {
                        markAsEvicted(record);
                    }
                }
            }
        } else {
            logger.log(Level.SEVERE, "Unknown backup operation " + req.operation);
        }
    }

    public boolean contains(Request req) {
        Data key = req.key;
        Data value = req.value;
        if (key != null) {
            Record record = getRecord(req);
            if (record == null) {
                return false;
            } else {
                if (record.isActive() && record.isValid()) {
                    if (value == null) {
                        return record.valueCount() > 0;
                    } else {
                        return record.containsValue(value);
                    }
                }
            }
        } else {
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                long now = System.currentTimeMillis();
                if (record.isActive() && record.isValid(now)) {
                    Address owner = concurrentMapManager.getPartitionManager().getOwner(record.getBlockId());
                    if (thisAddress.equals(owner)) {
                        if (record.containsValue(value)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public void containsValue(Request request) {
        if (isMultiMap()) {
            boolean found = false;
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                long now = System.currentTimeMillis();
                if (record.isActive() && record.isValid(now)) {
                    Address owner = concurrentMapManager.getPartitionOwner(record.getBlockId());
                    if (thisAddress.equals(owner)) {
                        if (record.containsValue(request.value)) {
                            found = true;
                        }
                    }
                }
            }
            request.response = found;
        } else {
            request.response = mapIndexService.containsValue(request.value);
        }
    }

    public CMapEntry getMapEntry(Request req) {
        Record record = getRecord(req);
        if (record == null || !record.isActive() || !record.isValid()) {
            return null;
        }
        return new CMapEntry(record);
    }

    public Data get(Request req) {
        Record record = getRecord(req);
        if (record == null)
            return null;
        if (!record.isActive()) return null;
        if (!record.isValid()) {
            if (record.isEvictable()) {
                return null;
            }
        }
        record.setLastAccessed();
        Data data = record.getValueData();
        Data returnValue = null;
        if (data != null) {
            returnValue = data;
        } else {
            if (record.getMultiValues() != null && record.getMultiValues().size() > 0) {
                Values values = new Values(record.getMultiValues());
                returnValue = toData(values);
            }
        }
        return returnValue;
    }

    public boolean add(Request req, boolean backup) {
        Record record = getRecord(req);
        if (record == null) {
            record = toRecord(req);
        } else {
            if (record.isActive() && req.operation == CONCURRENT_MAP_ADD_TO_SET) {
                return false;
            }
        }
        record.setActive(true);
        record.incrementVersion();
        if (!backup) {
            updateIndexes(record);
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_ADDED, null, record, req.caller);
        }
        return true;
    }

    void lock(Request request) {
        Data reqValue = request.value;
        Record rec = concurrentMapManager.ensureRecord(request);
        if (request.operation == CONCURRENT_MAP_TRY_LOCK_AND_GET) {
            if (reqValue == null) {
                request.value = rec.getValueData();
                if (rec.getMultiValues() != null) {
                    Values values = new Values(rec.getMultiValues());
                    request.value = toData(values);
                }
            }
        }
        rec.lock(request.lockThreadId, request.lockAddress);
        rec.incrementVersion();
        request.version = rec.getVersion();
        request.lockCount = rec.getLockCount();
        markAsActive(rec);
        request.response = Boolean.TRUE;
    }

    void unlock(Record record, Request request) {
        record.unlock(request.lockThreadId, request.lockAddress);
        fireScheduledActions(record);
    }

    void clearLock(Record record) {
        record.clearLock();
        fireScheduledActions(record);
    }

    void fireScheduledActions(Record record) {
        concurrentMapManager.checkServiceThread();
        if (record.getLockCount() == 0) {
            record.clearLock();
            while (record.hasScheduledAction()) {
                ScheduledAction sa = record.getScheduledActions().remove(0);
                node.clusterManager.deregisterScheduledAction(sa);
                if (!sa.expired()) {
                    sa.consume();
                    if (record.isLocked()) {
                        return;
                    }
                } else {
                    sa.onExpire();
                }
            }
        }
    }

    public void onMigrate(Record record) {
        if (record == null) return;
        List<ScheduledAction> lsScheduledActions = record.getScheduledActions();
        if (lsScheduledActions != null) {
            if (lsScheduledActions.size() > 0) {
                Iterator<ScheduledAction> it = lsScheduledActions.iterator();
                while (it.hasNext()) {
                    ScheduledAction sa = it.next();
                    if (sa.isValid() && !sa.expired()) {
                        sa.onMigrate();
                    }
                    sa.setValid(false);
                    node.clusterManager.deregisterScheduledAction(sa);
                    it.remove();
                }
            }
        }
    }

    public void onDisconnect(Address deadAddress) {
        if (deadAddress == null) return;
        if (lockEntireMap != null) {
            if (deadAddress.equals(lockEntireMap.getLockAddress())) {
                lockEntireMap = null;
            }
        }
    }

    public void onDisconnect(Record record, Address deadAddress) {
        if (record == null || deadAddress == null) return;
        List<ScheduledAction> lsScheduledActions = record.getScheduledActions();
        if (lsScheduledActions != null) {
            if (lsScheduledActions.size() > 0) {
                Iterator<ScheduledAction> it = lsScheduledActions.iterator();
                while (it.hasNext()) {
                    ScheduledAction sa = it.next();
                    if (deadAddress.equals(sa.getRequest().caller)) {
                        node.clusterManager.deregisterScheduledAction(sa);
                        sa.setValid(false);
                        it.remove();
                    }
                }
            }
        }
        if (record.getLockCount() > 0) {
            if (deadAddress.equals(record.getLockAddress())) {
                clearLock(record);
            }
        }
    }

    public void onRemoveMulti(Request req, Record record) {
        if (req.txnId != -1) {
            unlock(record, req);
        }
        record.incrementVersion();
        concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_REMOVED, record.getKeyData(), null, req.value, record.getListeners(), req.caller);
        req.version = record.getVersion();
        if (record.valueCount() == 0) {
            markAsRemoved(record);
        }
    }

    public void putMulti(Request req) {
        Record record = getRecord(req);
        if (record == null) {
            record = toRecord(req);
        } else {
            if (!record.isActive()) {
                markAsActive(record);
            }
            if (record.getMultiValues() == null) {
                record.setMultiValues(createMultiValuesCollection());
            }
            record.getMultiValues().add(new ValueHolder(req.value));
        }
//        updateIndexes(record);
        record.incrementVersion();
        concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_ADDED, record.getKeyData(), null, req.value, record.getListeners(), req.caller);
        if (req.txnId != -1) {
            unlock(record, req);
        }
        req.clearForResponse();
        req.version = record.getVersion();
    }

    boolean isApplicable(ClusterOperation operation, Request req, long now) {
        Record record = getRecord(req);
        if (ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT.equals(operation)) {
            return record == null || !record.isActive() || !record.isValid(now) || !record.hasValueData();
        } else if (ClusterOperation.CONCURRENT_MAP_REPLACE_IF_NOT_NULL.equals(operation)) {
            return record != null && record.isActive() && record.isValid(now) && record.hasValueData();
        }
        return true;
    }

    public void put(Request req) {
        long now = System.currentTimeMillis();
        boolean sendEvictEvent = false;
        Record evictedRecord = null;
        if (req.value == null) {
            req.value = new Data();
        }
        Record record = getRecord(req);
        if (record != null && !record.isValid(now)) {
            if (record.isActive() && record.isEvictable()) {
                sendEvictEvent = true;
                evictedRecord = createNewRecord(record.getKeyData(), record.getValueData());
            }
            record.setValue(null);
            record.setMultiValues(null);
        }
        if (req.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
            if (!isApplicable(CONCURRENT_MAP_PUT_IF_ABSENT, req, now)) {
                req.clearForResponse();
                req.response = record.getValueData();
                return;
            }
        } else if (req.operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL) {
            if (!isApplicable(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, req, now)) {
                // When request is remote, its value is used as response, so req.value should be cleared.
                req.value = null;
                return;
            }
        }
        Data oldValue = null;
        if (record == null) {
            record = createNewRecord(req.key, req.value);
            mapRecords.put(req.key, record);
        } else {
            markAsActive(record);
            oldValue = (record.isValid(now)) ? record.getValueData() : null;
            record.setValue(req.value);
            record.incrementVersion();
            record.setLastUpdated();
        }
        if (req.ttl > 0 && req.ttl < Long.MAX_VALUE) {
            record.setExpirationTime(req.ttl);
            ttlPerRecord = true;
        }
        if (sendEvictEvent) {
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_EVICTED, null, evictedRecord, req.caller);
        }
        if (oldValue == null) {
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_ADDED, null, record, req.caller);
        } else {
            fireInvalidation(record);
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_UPDATED, oldValue, record, req.caller);
        }
        if (req.txnId != -1 || req.operation == ClusterOperation.CONCURRENT_MAP_PUT_AND_UNLOCK) {
            unlock(record, req);
        }
        record.setIndexes(req.indexes, req.indexTypes);
        updateIndexes(record);
        markAsDirty(record);
        req.clearForResponse();
        req.version = record.getVersion();
        if (localUpdateListener != null && req.txnId != Long.MIN_VALUE) {
            localUpdateListener.recordUpdated(record);
        }
        if (req.operation == CONCURRENT_MAP_REPLACE_IF_SAME || req.operation == CONCURRENT_MAP_SET) {
            req.response = Boolean.TRUE;
        } else {
            req.response = oldValue;
        }
    }

    boolean isMapForQueue() {
        return mapForQueue;
    }

    void sendKeyToMaster(Data key) {
        String queueName = name.substring(2);
        if (concurrentMapManager.isMaster()) {
            node.blockingQueueManager.doAddKey(queueName, key, 0);
        } else {
            Packet packet = concurrentMapManager.obtainPacket();
            packet.name = queueName;
            packet.setKey(key);
            packet.operation = ClusterOperation.BLOCKING_OFFER_KEY;
            packet.longValue = 0;
            boolean sent = concurrentMapManager.send(packet, concurrentMapManager.getMasterAddress());
        }
    }

    private void executeStoreUpdate(final Set<Record> dirtyRecords) {
        if (dirtyRecords.size() > 0) {
            concurrentMapManager.storeExecutor.execute(new Runnable() {
                public void run() {
                    try {
                        Set<Object> keysToDelete = new HashSet<Object>();
                        Set<Record> toStore = new HashSet<Record>();
                        Map<Object, Object> updates = new HashMap<Object, Object>();
                        for (Record dirtyRecord : dirtyRecords) {
                            if (!dirtyRecord.isActive()) {
                                keysToDelete.add(dirtyRecord.getKey());
                            } else {
                                toStore.add(dirtyRecord);
                                updates.put(dirtyRecord.getKey(), dirtyRecord.getValue());
                            }
                        }
                        if (keysToDelete.size() == 1) {
                            store.delete(keysToDelete.iterator().next());
                        } else if (keysToDelete.size() > 1) {
                            store.deleteAll(keysToDelete);
                        }
                        if (updates.size() == 1) {
                            Map.Entry entry = updates.entrySet().iterator().next();
                            store.store(entry.getKey(), entry.getValue());
                        } else if (updates.size() > 1) {
                            store.storeAll(updates);
                        }
                        for (Record stored : toStore) {
                            stored.setLastStoredTime(System.currentTimeMillis());
                        }
                    } catch (Exception e) {
                        for (Record dirtyRecord : dirtyRecords) {
                            dirtyRecord.setDirty(true);
                        }
                    }
                }
            });
        }
    }

    private void purgeIfNotOwnedOrBackup(Collection<Record> records) {
        PartitionManager partitionManager = concurrentMapManager.getPartitionManager();
        for (Record record : records) {
            if (partitionManager.shouldPurge(record.getBlockId(), backupCount)) {
                mapIndexService.remove(record);
                mapRecords.remove(record.getKeyData());
            }
        }
    }

    Record getOwnedRecord(Data key) {
        PartitionServiceImpl partitionService = concurrentMapManager.partitionServiceImpl;
        PartitionServiceImpl.PartitionProxy partition = partitionService.getPartition(concurrentMapManager.getPartitionId(key));
        Member ownerNow = partition.getOwner();
        if (ownerNow != null && !partition.isMigrating() && ownerNow.localMember()) {
            return getRecord(key);
        }
        return null;
    }

    boolean isBackup(Record record) {
        PartitionServiceImpl partitionService = concurrentMapManager.partitionServiceImpl;
        PartitionServiceImpl.PartitionProxy partition = partitionService.getPartition(record.getBlockId());
        Member owner = partition.getOwner();
        if (owner == null) return partition.isMigrating();
        return !owner.localMember() && partition.isMigrating();
    }

    public int size() {
        if (maxIdle > 0 || ttl > 0 || ttlPerRecord || isList() || isMultiMap()) {
            long now = System.currentTimeMillis();
            int size = 0;
            Collection<Record> records = mapIndexService.getOwnedRecords();
            for (Record record : records) {
                if (record.isActive() && record.isValid(now)) {
                    size += record.valueCount();
                }
            }
            return size;
        } else {
//            System.out.println("size " + mapIndexService.size());
            return mapIndexService.size();
        }
    }

    public int size(int expectedPartitionVersion) {
        PartitionManager partitionManager = concurrentMapManager.partitionManager;
        if (partitionManager.getVersion() != expectedPartitionVersion) return -1;
        long now = System.currentTimeMillis();
        int size = 0;
        Collection<Record> records = mapRecords.values();
        for (Record record : records) {
            Address owner = concurrentMapManager.getPartitionOwner(record.getBlockId());
            if (owner != null && thisAddress.equals(owner)) {
                if (record.isActive() && record.isValid(now)) {
                    size += record.valueCount();
                }
            }
        }
        if (partitionManager.getVersion() != expectedPartitionVersion) return -1;
        return size;
    }

    public void collectScheduledLocks(Map<Object, DistributedLock> lockOwners, Map<Object, DistributedLock> lockRequested) {
        Collection<Record> records = mapRecords.values();
        for (Record record : records) {
            DistributedLock dLock = record.getLock();
            if (dLock != null && dLock.isLocked()) {
                List<ScheduledAction> scheduledActions = record.getScheduledActions();
                if (scheduledActions != null) {
                    for (ScheduledAction scheduledAction : scheduledActions) {
                        Request request = scheduledAction.getRequest();
                        if (ClusterOperation.CONCURRENT_MAP_LOCK.equals(request.operation)) {
                            lockOwners.put(record.getKey(), dLock);
                            lockRequested.put(record.getKey(), new DistributedLock(request.lockAddress, request.lockThreadId));
                        }
                    }
                }
            }
        }
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

    LocalMapStatsImpl getLocalMapStats() {
        LocalMapStatsImpl localMapStats = new LocalMapStatsImpl();
        long now = System.currentTimeMillis();
        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long markedAsRemovedEntryCount = 0;
        long dirtyCount = 0;
        long ownedEntryMemoryCost = 0;
        long backupEntryMemoryCost = 0;
        long markedAsRemovedMemoryCost = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        long lockWaitCount = 0;
        ClusterImpl clusterImpl = node.getClusterImpl();
        final Collection<Record> records = mapRecords.values();
        final PartitionManager partitionManager = concurrentMapManager.partitionManager;
        for (Record record : records) {
            if (!record.isActive() || !record.isValid(now)) {
                markedAsRemovedEntryCount++;
                markedAsRemovedMemoryCost += record.getCost();
            } else {
                PartitionInfo partition = partitionManager.getPartition(record.getBlockId());
                Address owner = partition.getOwner();
                if (owner != null && thisAddress.equals(owner)) {
                    if (store != null && record.getLastStoredTime() < Math.max(record.getLastUpdateTime(), record.getCreationTime())) {
                        dirtyCount++;
                    }
                    ownedEntryCount += record.valueCount();
                    ownedEntryMemoryCost += record.getCost();
                    localMapStats.setLastAccessTime(record.getLastAccessTime());
                    localMapStats.setLastUpdateTime(record.getLastUpdateTime());
                    hits += record.getHits();
                    if (record.isLocked()) {
                        lockedEntryCount++;
                        lockWaitCount += record.getScheduledActionCount();
                    }
                } else if (partition.isBackup(thisAddress, backupCount)) {
                    if (record.valueCount() <= 0) throw new RuntimeException();
                    backupEntryCount += record.valueCount();
                    backupEntryMemoryCost += record.getCost();
                }
            }
        }
//        System.out.println(thisAddress + "  >> " + ownedEntryCount + "  backup: " + backupEntryCount + "   map.size " + mapRecords.size());
        localMapStats.setDirtyEntryCount(zeroOrPositive(dirtyCount));
        localMapStats.setMarkedAsRemovedEntryCount(zeroOrPositive(markedAsRemovedEntryCount));
        localMapStats.setMarkedAsRemovedMemoryCost(zeroOrPositive(markedAsRemovedMemoryCost));
        localMapStats.setLockWaitCount(zeroOrPositive(lockWaitCount));
        localMapStats.setLockedEntryCount(zeroOrPositive(lockedEntryCount));
        localMapStats.setHits(zeroOrPositive(hits));
        localMapStats.setOwnedEntryCount(zeroOrPositive(ownedEntryCount));
        localMapStats.setBackupEntryCount(zeroOrPositive(backupEntryCount));
        localMapStats.setOwnedEntryMemoryCost(zeroOrPositive(ownedEntryMemoryCost));
        localMapStats.setBackupEntryMemoryCost(zeroOrPositive(backupEntryMemoryCost));
        localMapStats.setLastEvictionTime(zeroOrPositive(clusterImpl.getClusterTimeFor(lastEvictionTime)));
        localMapStats.setCreationTime(zeroOrPositive(clusterImpl.getClusterTimeFor(creationTime)));
        return localMapStats;
    }

    private static long zeroOrPositive(long value) {
        return (value > 0) ? value : 0;
    }

    /**
     * Comparator that never returns 0. It is
     * either 1 or -1;
     */
    class ComparatorWrapper implements Comparator<MapEntry> {
        final Comparator<MapEntry> comparator;

        ComparatorWrapper(Comparator<MapEntry> comparator) {
            this.comparator = comparator;
        }

        public int compare(MapEntry o1, MapEntry o2) {
            int result = comparator.compare(o1, o2);
            if (result == 0) {
                Record r1 = (Record) o1;
                Record r2 = (Record) o2;
                // we don't want to return 0 here.
                return (r1.getId() > r2.getId()) ? 1 : -1;
            } else {
                return result;
            }
        }
    }

    void evict(int percentage) {
        final long now = System.currentTimeMillis();
        final Collection<Record> records = mapRecords.values();
        Comparator<MapEntry> comparator = evictionComparator;
        if (comparator == null) {
            comparator = new ComparatorWrapper(LRU_COMPARATOR);
        }
        final PartitionServiceImpl partitionService = concurrentMapManager.partitionServiceImpl;
        final Set<Record> sortedRecords = new TreeSet<Record>(new ComparatorWrapper(comparator));
        final Set<Record> recordsToEvict = new HashSet<Record>();
        for (Record record : records) {
            PartitionServiceImpl.PartitionProxy partition = partitionService.getPartition(record.getBlockId());
            Member owner = partition.getOwner();
            if (owner != null && !partition.isMigrating()) {
                boolean owned = owner.localMember();
                if (owned) {
                    if (store != null && writeDelayMillis > 0 && record.isDirty()) {
                    } else if (shouldPurgeRecord(record, now)) {
                    } else if (record.isActive() && !record.isValid(now)) {
                        recordsToEvict.add(record);  // expired records
                    } else if (record.isActive() && record.isEvictable()) {
                        sortedRecords.add(record);   // sorting for eviction
                    }
                }
            }
        }
        int numberOfRecordsToEvict = sortedRecords.size() * percentage / 100;
        int evictedCount = 0;
        for (Record record : sortedRecords) {
            recordsToEvict.add(record);
            if (++evictedCount >= numberOfRecordsToEvict) {
                break;
            }
        }
        executeEviction(recordsToEvict);
    }

    class MaxSizePerJVMPolicy implements MapMaxSizePolicy {
        protected final MaxSizeConfig maxSizeConfig;

        MaxSizePerJVMPolicy(MaxSizeConfig maxSizeConfig) {
            this.maxSizeConfig = maxSizeConfig;
        }

        public int getMaxSize() {
            return maxSizeConfig.getSize();
        }

        public boolean overCapacity() {
            return getMaxSize() <= mapIndexService.size();
        }

        public MaxSizeConfig getMaxSizeConfig() {
            return maxSizeConfig;
        }
    }

    class MaxSizeHeapPolicy extends MaxSizePerJVMPolicy {
        long memoryLimit = 0;

        MaxSizeHeapPolicy(MaxSizeConfig maxSizeConfig) {
            super(maxSizeConfig);
            memoryLimit = maxSizeConfig.getSize() * 1000 * 1000; // MB to byte
        }

        public boolean overCapacity() {
            boolean over = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) > memoryLimit;
            if (over) {
                Runtime.getRuntime().gc();
            }
            return over;
        }
    }

    class MaxSizeHeapPercentagePolicy extends MaxSizePerJVMPolicy {

        MaxSizeHeapPercentagePolicy(MaxSizeConfig maxSizeConfig) {
            super(maxSizeConfig);
        }

        public boolean overCapacity() {
            long total = Runtime.getRuntime().totalMemory();
            long free = Runtime.getRuntime().freeMemory();
            int usedPercentage = (int) (((total - free) / total) * 100D);
            boolean over = usedPercentage > maxSizeConfig.getSize();
            if (over) {
                Runtime.getRuntime().gc();
            }
            return over;
        }
    }

    class MaxSizeClusterWidePolicy extends MaxSizePerJVMPolicy {

        MaxSizeClusterWidePolicy(MaxSizeConfig maxSizeConfig) {
            super(maxSizeConfig);
        }

        @Override
        public int getMaxSize() {
            final int maxSize = maxSizeConfig.getSize();
            final int clusterMemberSize = node.getClusterImpl().getMembers().size();
            final int memberCount = (clusterMemberSize == 0) ? 1 : clusterMemberSize;
            return maxSize / memberCount;
        }
    }

    class MaxSizePartitionsWidePolicy extends MaxSizePerJVMPolicy {

        MaxSizePartitionsWidePolicy(MaxSizeConfig maxSizeConfig) {
            super(maxSizeConfig);
        }

        @Override
        public int getMaxSize() {
            final int maxSize = maxSizeConfig.getSize();
            if (maxSize == Integer.MAX_VALUE || maxSize == 0) return Integer.MAX_VALUE;
            if (node.getClusterImpl().getMembers().size() < 2) {
                return maxSize;
            } else {
                PartitionServiceImpl partitionService = concurrentMapManager.partitionServiceImpl;
                int partitionCount = partitionService.getPartitions().size();
                int ownedPartitionCount = partitionService.getOwnedPartitionCount();
                if (partitionCount < 1 || ownedPartitionCount < 1) return maxSize;
                return (maxSize * ownedPartitionCount) / partitionCount;
            }
        }
    }

    void startCleanup(boolean forced) {
        if (cleanupActive.compareAndSet(false, true)) {
            try {
                final long now = System.currentTimeMillis();
                if (nearCache != null) {
                    nearCache.evict(now, false);
                }
                final Set<Record> recordsDirty = new HashSet<Record>();
                final Set<Record> recordsUnknown = new HashSet<Record>();
                final Set<Record> recordsToPurge = new HashSet<Record>();
                final Set<Record> recordsToEvict = new HashSet<Record>();
                final Set<Record> sortedRecords = new TreeSet<Record>(new ComparatorWrapper(evictionComparator));
                final Collection<Record> records = mapRecords.values();
                final boolean overCapacity = maxSizePolicy.overCapacity();
                final boolean evictionAware = evictionComparator != null && overCapacity;
                int recordsStillOwned = 0;
                int backupPurgeCount = 0;
                PartitionManager partitionManager = concurrentMapManager.partitionManager;
                for (Record record : records) {
                    PartitionInfo partition = partitionManager.getPartition(record.getBlockId());
                    Address owner = partition.getOwner();
                    boolean owned = (owner != null && thisAddress.equals(owner));
                    boolean ownedOrBackup = partition.isOwnerOrBackup(thisAddress, backupCount);
                    if (owner != null && !partitionManager.isMigrating(partition.getPartitionId())) {
                        if (owned) {
                            if (store != null && writeDelayMillis > 0 && record.isDirty()) {
                                if (now > record.getWriteTime()) {
                                    recordsDirty.add(record);
                                    record.setDirty(false);
                                }
                            } else if (shouldPurgeRecord(record, now)) {
                                recordsToPurge.add(record);  // removed records
                            } else if (record.isActive() && !record.isValid(now)) {
                                recordsToEvict.add(record);  // expired records
                            } else if (evictionAware && record.isActive() && record.isEvictable()) {
                                sortedRecords.add(record);   // sorting for eviction
                                recordsStillOwned++;
                            }
                        } else if (ownedOrBackup) {
                            if (shouldPurgeRecord(record, now)) {
                                recordsToPurge.add(record);
                                backupPurgeCount++;
                            }
                        } else {
                            recordsUnknown.add(record);
                        }
                    }
                }
                if (evictionAware && (forced || overCapacity)) {
                    int numberOfRecordsToEvict = (int) (recordsStillOwned * evictionRate);
                    int evictedCount = 0;
                    for (Record record : sortedRecords) {
                        if (record.isActive() && record.isEvictable()) {
                            recordsToEvict.add(record);
                            if (++evictedCount >= numberOfRecordsToEvict) {
                                break;
                            }
                        }
                    }
                }
                Level levelLog = (concurrentMapManager.LOG_STATE) ? Level.INFO : Level.FINEST;
                logger.log(levelLog, name + " Cleanup "
                        + ", dirty:" + recordsDirty.size()
                        + ", purge:" + recordsToPurge.size()
                        + ", evict:" + recordsToEvict.size()
                        + ", unknown:" + recordsUnknown.size()
                        + ", stillOwned:" + recordsStillOwned
                        + ", backupPurge:" + backupPurgeCount
                );
                executeStoreUpdate(recordsDirty);
                executeEviction(recordsToEvict);
                executePurge(recordsToPurge);
                executePurgeUnknowns(recordsUnknown);
            } finally {
                cleanupActive.set(false);
            }
        }
    }

    private void executePurgeUnknowns(final Set<Record> recordsUnknown) {
        if (recordsUnknown.size() > 0) {
            concurrentMapManager.enqueueAndReturn(new Processable() {
                public void process() {
                    purgeIfNotOwnedOrBackup(recordsUnknown);
                }
            });
        }
    }

    private void executePurge(final Set<Record> recordsToPurge) {
        if (recordsToPurge.size() > 0) {
            concurrentMapManager.enqueueAndReturn(new Processable() {
                public void process() {
                    final long now = System.currentTimeMillis();
                    for (Record recordToPurge : recordsToPurge) {
                        if (shouldPurgeRecord(recordToPurge, now)) {
                            removeAndPurgeRecord(recordToPurge);
                        }
                    }
                }
            });
        }
    }

    boolean shouldPurgeRecord(Record record, long now) {
        return !record.isActive() && shouldRemove(record, now);
    }

    private void executeEviction(Collection<Record> lsRecordsToEvict) {
        if (lsRecordsToEvict != null && lsRecordsToEvict.size() > 0) {
            logger.log(Level.FINEST, lsRecordsToEvict.size() + " evicting");
            for (final Record recordToEvict : lsRecordsToEvict) {
                concurrentMapManager.evictAsync(name, recordToEvict.getKeyData());
            }
        }
    }

    void fireInvalidation(Record record) {
        if (nearCache != null && nearCache.shouldInvalidateOnChange()) {
            for (MemberImpl member : concurrentMapManager.lsMembers) {
                if (!member.localMember()) {
                    if (member.getAddress() != null) {
                        Packet packet = concurrentMapManager.obtainPacket();
                        packet.name = getName();
                        packet.setKey(record.getKeyData());
                        packet.operation = ClusterOperation.CONCURRENT_MAP_INVALIDATE;
                        boolean sent = concurrentMapManager.send(packet, member.getAddress());
                        if (!sent) {
                            concurrentMapManager.releasePacket(packet);
                        }
                    }
                }
            }
            nearCache.invalidate(record.getKeyData());
        }
    }

    Record getRecord(Request req) {
        if (req.record == null || !req.record.isActive()) {
            req.record = mapRecords.get(req.key);
        }
        return req.record;
    }

    Record toRecord(Request req) {
        Record record = getRecord(req);
        if (record == null) {
            if (isMultiMap()) {
                record = createNewRecord(req.key, null);
                record.setMultiValues(createMultiValuesCollection());
                if (req.value != null) {
                    record.getMultiValues().add(new ValueHolder(req.value));
                }
            } else {
                record = createNewRecord(req.key, req.value);
            }
            mapRecords.put(req.key, record);
        } else {
            if (req.value != null) {
                if (isMultiMap()) {
                    if (record.getMultiValues() == null) {
                        record.setMultiValues(createMultiValuesCollection());
                    }
                    record.getMultiValues().add(new ValueHolder(req.value));
                } else {
                    record.setValue(req.value);
                }
            }
        }
        record.setIndexes(req.indexes, req.indexTypes);
        if (req.lockCount >= 0) {
            DistributedLock lock = new DistributedLock(req.lockAddress, req.lockThreadId, req.lockCount);
            record.setLock(lock);
        }
        return record;
    }

    public boolean removeItem(Request req) {
        Record record = getRecord(req);
        if (record == null) {
            return false;
        }
        if (req.txnId != -1) {
            unlock(record, req);
        }
        boolean removed = false;
        if (record.hasValueData()) {
            removed = true;
        } else if (record.getMultiValues() != null) {
            removed = true;
        }
        if (removed) {
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_REMOVED, null, record, req.caller);
            record.incrementVersion();
        }
        req.version = record.getVersion();
        markAsRemoved(record);
        return true;
    }

    boolean evict(Request req) {
        Record record = getRecord(req.key);
        long now = System.currentTimeMillis();
        if (record != null && record.isActive() && record.valueCount() > 0) {
            concurrentMapManager.checkServiceThread();
            fireInvalidation(record);
            concurrentMapManager.fireMapEvent(mapListeners, EntryEvent.TYPE_EVICTED, null, record, req.caller);
            record.incrementVersion();
            markAsEvicted(record);
            req.clearForResponse();
            req.version = record.getVersion();
            lastEvictionTime = now;
            return true;
        }
        return false;
    }

    public void remove(Request req) {
        Record record = getRecord(req);
        if (record == null) {
            req.clearForResponse();
            return;
        }
        try {
            if (!record.isActive()) {
                return;
            }
            if (!record.isValid()) {
                if (record.isEvictable()) {
                    return;
                }
            }
            if (req.value != null) {
                if (record.hasValueData()) {
                    if (!record.getValueData().equals(req.value)) {
                        return;
                    }
                }
            }
            Data oldValue = record.getValueData();
            if (oldValue == null && record.getMultiValues() != null && record.getMultiValues().size() > 0) {
                Values values = new Values(record.getMultiValues());
                oldValue = toData(values);
            }
            if (oldValue != null) {
                fireInvalidation(record);
                concurrentMapManager.fireMapEvent(mapListeners, getName(), EntryEvent.TYPE_REMOVED, record.getKeyData(), null, oldValue, record.getListeners(), req.caller);
                record.incrementVersion();
            }
            markAsRemoved(record);
            if (localUpdateListener != null && req.txnId != Long.MIN_VALUE) {
                localUpdateListener.recordUpdated(record);
            }
            req.clearForResponse();
            req.version = record.getVersion();
            req.response = oldValue;
        } finally {
            if (req.txnId != -1) {
                unlock(record, req);
            }
        }
    }

    void reset(boolean invalidate) {
        for (Record record : mapRecords.values()) {
            if (record.hasScheduledAction()) {
                List<ScheduledAction> lsScheduledActions = record.getScheduledActions();
                if (lsScheduledActions != null) {
                    for (ScheduledAction scheduledAction : lsScheduledActions) {
                        scheduledAction.setValid(false);
                    }
                }
            }
            // on destroy; invalidate all records
            // on restart; invalidation occurs after merge
            if (invalidate) {
                record.invalidate();
            }
        }
        if (nearCache != null) {
            nearCache.reset();
        }
        mapRecords.clear();
        mapIndexService.clear();
    }

    void destroy() {
        reset(true);
        node.listenerManager.removeAllRegisteredListeners(getName());
        if (mapStoreWrapper != null) {
            try {
                mapStoreWrapper.destroy();
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    void markAsDirty(Record record) {
        if (!record.isDirty()) {
            record.setDirty(true);
            if (writeDelayMillis > 0) {
                record.setWriteTime(System.currentTimeMillis() + writeDelayMillis);
            }
        }
    }

    void markAsActive(Record record) {
        long now = System.currentTimeMillis();
        if (!record.isActive() || !record.isValid(now)) {
            record.setActive();
            record.setCreationTime(now);
            record.setLastUpdateTime(0L);
            record.setExpirationTime(ttl);
        }
    }

    boolean shouldRemove(Record record, long now) {
        return record.isRemovable() && ((now - record.getRemoveTime()) > removeDelayMillis);
    }

    void markAsRemoved(Record record) {
        if (record.isActive()) {
            record.markRemoved();
        }
        record.setValue(null);
        record.setMultiValues(null);
        updateIndexes(record);
        markAsDirty(record);
    }

    /**
     * same as markAsRemoved but it doesn't
     * mark the entry as 'dirty' because
     * inactive and dirty records are deleted
     * from mapStore
     *
     * @param record
     */
    void markAsEvicted(Record record) {
        if (record.isActive()) {
            record.markRemoved();
        }
        record.setValue(null);
        record.setMultiValues(null);
        updateIndexes(record);
    }

    void removeAndPurgeRecord(Record record) {
        mapRecords.remove(record.getKeyData());
        mapIndexService.remove(record);
    }

    void updateIndexes(Record record) {
        mapIndexService.index(record);
    }

    Record createNewRecord(Data key, Data value) {
        if (key == null || key.size() == 0) {
            throw new RuntimeException("Cannot create record from a 0 size key: " + key);
        }
        int blockId = concurrentMapManager.getPartitionId(key);
        return concurrentMapManager.recordFactory.createNewRecord(this, blockId, key, value,
                ttl, maxIdle, concurrentMapManager.newRecordId());
    }

    public void addListener(Data key, Address address, boolean includeValue) {
        if (key == null || key.size() == 0) {
            mapListeners.put(address, includeValue);
        } else {
            Record rec = getRecord(key);
            if (rec == null) {
                rec = createNewRecord(key, null);
                mapRecords.put(key, rec);
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

    public void appendState(StringBuffer sbState) {
        sbState.append("\nCMap [");
        sbState.append(name);
        sbState.append("] r:");
        sbState.append(mapRecords.size());
        if (nearCache != null) {
            nearCache.appendState(sbState);
        }
        mapIndexService.appendState(sbState);
        for (Record record : mapRecords.values()) {
            if (record.isLocked()) {
                sbState.append("\nLocked Record by ").append(record.getLock());
            }
        }
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public Node getNode() {
        return node;
    }

    public static class CMapEntry implements HazelcastInstanceAware, MapEntry, DataSerializable {
        private long cost = 0;
        private long expirationTime = 0;
        private long lastAccessTime = 0;
        private long lastUpdateTime = 0;
        private long lastStoredTime = 0;
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

        public CMapEntry(Record record) {
            this.cost = record.getCost();
            this.expirationTime = record.getExpirationTime();
            this.lastAccessTime = record.getLastAccessTime();
            this.lastUpdateTime = record.getLastUpdateTime();
            this.creationTime = record.getCreationTime();
            this.lastStoredTime = record.getLastStoredTime();
            this.version = record.getVersion();
            this.hits = record.getHits();
            this.valid = record.isValid();
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeLong(cost);
            out.writeLong(expirationTime);
            out.writeLong(lastAccessTime);
            out.writeLong(lastUpdateTime);
            out.writeLong(creationTime);
            out.writeLong(lastStoredTime);
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
            lastStoredTime = in.readLong();
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

        public long getLastStoredTime() {
            return lastStoredTime;
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
                value = ((MProxy) factory.getOrCreateProxyByName(name)).get(key);
            }
            return value;
        }

        public Object setValue(Object value) {
            Object oldValue = this.value;
            FactoryImpl factory = (FactoryImpl) hazelcastInstance;
            ((MProxy) factory.getOrCreateProxyByName(name)).put(key, value);
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
        return "CMap [" + getName() + "]";
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public MapIndexService getMapIndexService() {
        return mapIndexService;
    }

    private Collection<ValueHolder> createMultiValuesCollection() {
        if (multiMapSet) {
            return new ConcurrentHashSet<ValueHolder>();
        } else {
            return new CopyOnWriteArrayList<ValueHolder>();
        }
    }
}
