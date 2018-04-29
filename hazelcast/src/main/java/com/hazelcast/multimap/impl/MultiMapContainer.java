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

package com.hazelcast.multimap.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * MultiMap container which holds a map of {@link MultiMapValue}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class MultiMapContainer extends MultiMapContainerSupport {

    private static final int ID_PROMOTION_OFFSET = 100000;

    private final DistributedObjectNamespace lockNamespace;
    private final LockStore lockStore;
    private final int partitionId;
    private final long creationTime;
    private final ObjectNamespace objectNamespace;

    private long idGen;

    // these fields are volatile since they can be read by other threads than the partition-thread
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;

    public MultiMapContainer(String name, MultiMapService service, int partitionId) {
        super(name, service.getNodeEngine());
        this.partitionId = partitionId;
        this.lockNamespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name);
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null : lockService.createLockStore(partitionId, lockNamespace);
        this.creationTime = currentTimeMillis();
        this.objectNamespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name);
    }

    public boolean canAcquireLock(Data dataKey, String caller, long threadId) {
        return lockStore != null && lockStore.canAcquireLock(dataKey, caller, threadId);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean isTransactionallyLocked(Data key) {
        return lockStore != null && lockStore.shouldBlockReads(key);
    }

    public boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads) {
        return lockStore != null && lockStore.txnLock(key, caller, threadId, referenceId, ttl, blockReads);
    }

    public boolean unlock(Data key, String caller, long threadId, long referenceId) {
        return lockStore != null && lockStore.unlock(key, caller, threadId, referenceId);
    }

    public boolean forceUnlock(Data key) {
        return lockStore != null && lockStore.forceUnlock(key);
    }

    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public String getLockOwnerInfo(Data dataKey) {
        return lockStore != null ? lockStore.getOwnerInfo(dataKey) : null;
    }

    public long nextId() {
        return idGen++;
    }

    public void setId(long newValue) {
        idGen = newValue + ID_PROMOTION_OFFSET;
    }

    public boolean delete(Data dataKey) {
        return multiMapValues.remove(dataKey) != null;
    }

    public Collection<MultiMapRecord> remove(Data dataKey, boolean copyOf) {
        MultiMapValue multiMapValue = multiMapValues.remove(dataKey);
        return multiMapValue != null ? multiMapValue.getCollection(copyOf) : null;
    }

    public Set<Data> keySet() {
        Set<Data> keySet = multiMapValues.keySet();
        return new HashSet<Data>(keySet);
    }

    public Collection<MultiMapRecord> values() {
        Collection<MultiMapRecord> valueCollection = new LinkedList<MultiMapRecord>();
        for (MultiMapValue multiMapValue : multiMapValues.values()) {
            valueCollection.addAll(multiMapValue.getCollection(false));
        }
        return valueCollection;
    }

    public boolean containsKey(Data key) {
        return multiMapValues.containsKey(key);
    }

    public boolean containsEntry(boolean binary, Data key, Data value) {
        MultiMapValue multiMapValue = multiMapValues.get(key);
        if (multiMapValue == null) {
            return false;
        }
        MultiMapRecord record = new MultiMapRecord(binary ? value : nodeEngine.toObject(value));
        return multiMapValue.getCollection(false).contains(record);
    }

    public boolean containsValue(boolean binary, Data value) {
        for (Data key : multiMapValues.keySet()) {
            if (containsEntry(binary, key, value)) {
                return true;
            }
        }
        return false;
    }

    public Map<Data, Collection<MultiMapRecord>> copyCollections() {
        Map<Data, Collection<MultiMapRecord>> map = createHashMap(multiMapValues.size());
        for (Map.Entry<Data, MultiMapValue> entry : multiMapValues.entrySet()) {
            Data key = entry.getKey();
            Collection<MultiMapRecord> col = entry.getValue().getCollection(true);
            map.put(key, col);
        }
        return map;
    }

    public int size() {
        int size = 0;
        for (MultiMapValue multiMapValue : multiMapValues.values()) {
            size += multiMapValue.getCollection(false).size();
        }
        return size;
    }

    public int clear() {
        Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        Map<Data, MultiMapValue> lockedKeys = createHashMap(locks.size());
        for (Data key : locks) {
            MultiMapValue multiMapValue = multiMapValues.get(key);
            if (multiMapValue != null) {
                lockedKeys.put(key, multiMapValue);
            }
        }
        int numberOfAffectedEntries = multiMapValues.size() - lockedKeys.size();
        multiMapValues.clear();
        multiMapValues.putAll(lockedKeys);
        return numberOfAffectedEntries;
    }

    public void destroy() {
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionId, lockNamespace);
        }
        multiMapValues.clear();
    }

    public void access() {
        lastAccessTime = currentTimeMillis();
    }

    public void update() {
        lastUpdateTime = currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLockedCount() {
        return lockStore.getLockedKeys().size();
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Merges the given {@link MultiMapMergeContainer} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergeContainer the {@link MultiMapMergeContainer} instance to merge
     * @param mergePolicy    the {@link SplitBrainMergePolicy} instance to apply
     * @return the used {@link MultiMapValue} if merge is applied, otherwise {@code null}
     */
    public MultiMapValue merge(MultiMapMergeContainer mergeContainer,
                               SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes> mergePolicy) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        serializationService.getManagedContext().initialize(mergePolicy);

        MultiMapMergeTypes mergingEntry = createMergingEntry(serializationService, mergeContainer);
        MultiMapValue existingValue = getMultiMapValueOrNull(mergeContainer.getKey());
        if (existingValue == null) {
            return mergeNewValue(mergePolicy, mergingEntry);
        }
        return mergeExistingValue(mergePolicy, mergingEntry, existingValue, serializationService);
    }

    private MultiMapValue mergeNewValue(SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes> mergePolicy,
                                        MultiMapMergeTypes mergingEntry) {
        Collection<Object> newValues = mergePolicy.merge(mergingEntry, null);
        if (newValues != null && !newValues.isEmpty()) {
            MultiMapValue mergedValue = getOrCreateMultiMapValue(mergingEntry.getKey());
            Collection<MultiMapRecord> records = mergedValue.getCollection(false);
            createNewMultiMapRecords(records, newValues);
            if (newValues.equals(mergingEntry.getValue())) {
                setMergedStatistics(mergingEntry, mergedValue);
            }
            return mergedValue;
        }
        return null;
    }

    private MultiMapValue mergeExistingValue(SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes> mergePolicy,
                                             MultiMapMergeTypes mergingEntry, MultiMapValue existingValue,
                                             SerializationService ss) {
        Collection<MultiMapRecord> existingRecords = existingValue.getCollection(false);

        Data dataKey = mergingEntry.getKey();
        MultiMapMergeTypes existingEntry = createMergingEntry(ss, this, dataKey, existingRecords, existingValue.getHits());
        Collection<Object> newValues = mergePolicy.merge(mergingEntry, existingEntry);
        if (newValues == null || newValues.isEmpty()) {
            existingRecords.clear();
            multiMapValues.remove(dataKey);
        } else if (!newValues.equals(existingRecords)) {
            existingRecords.clear();
            createNewMultiMapRecords(existingRecords, newValues);
            if (newValues.equals(mergingEntry.getValue())) {
                setMergedStatistics(mergingEntry, existingValue);
            }
        }
        return existingValue;
    }

    private void createNewMultiMapRecords(Collection<MultiMapRecord> records, Collection<Object> values) {
        boolean isBinary = config.isBinary();
        SerializationService serializationService = nodeEngine.getSerializationService();

        for (Object value : values) {
            long recordId = nextId();
            MultiMapRecord record = new MultiMapRecord(recordId, isBinary ? serializationService.toData(value) : value);
            records.add(record);
        }
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void setMergedStatistics(MultiMapMergeTypes mergingEntry, MultiMapValue multiMapValue) {
        multiMapValue.setHits(mergingEntry.getHits());
        lastAccessTime = Math.max(lastAccessTime, mergingEntry.getLastAccessTime());
        lastUpdateTime = Math.max(lastUpdateTime, mergingEntry.getLastUpdateTime());
    }
}
