/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.replicatedmap.impl.ReplicatedMapEventPublishingService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.ReplicateUpdateOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * This is the base class for all {@link ReplicatedRecordStore} implementations
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class AbstractReplicatedRecordStore<K, V> extends AbstractBaseReplicatedRecordStore<K, V> {

    public AbstractReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
        super(name, replicatedMapService, partitionId);
    }

    @Override
    public Object remove(Object key) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        Object old = remove(storage, key);
        storage.incrementVersion();
        return old;
    }

    @Override
    public Object removeWithVersion(Object key, long version) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        Object old = remove(storage, key);
        storage.setVersion(version);
        return old;
    }

    @SuppressWarnings("unchecked")
    private Object remove(InternalReplicatedMapStorage<K, V> storage, Object key) {
        isNotNull(key, "key");
        long startNanos = Timer.nanos();
        V oldValue;
        K marshalledKey = (K) marshall(key);
        ReplicatedRecord<K, V> current = storage.get(marshalledKey);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = current.getValueInternal();
            storage.remove(marshalledKey, current);
        }
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementRemovesNanos(Timer.nanosElapsed(startNanos));
        }
        cancelTtlEntry(marshalledKey);
        return oldValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void evict(Object key) {
        isNotNull(key, "key");
        long startNanos = Timer.nanos();
        V oldValue;
        K marshalledKey = (K) marshall(key);
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        ReplicatedRecord<K, V> current = storage.get(marshalledKey);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = current.getValueInternal();
            storage.remove(marshalledKey, current);
        }
        Data dataKey = nodeEngine.toData(key);
        Data dataOldValue = nodeEngine.toData(oldValue);
        ReplicatedMapEventPublishingService eventPublishingService = replicatedMapService.getEventPublishingService();
        eventPublishingService.fireEntryListenerEvent(dataKey, dataOldValue, null, EVICTED, name, nodeEngine.getThisAddress());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementRemovesNanos(Timer.nanosElapsed(startNanos));
        }
    }

    @Override
    public Object get(Object key) {
        isNotNull(key, "key");
        long startNanos = Timer.nanos();
        ReplicatedRecord<K, V> replicatedRecord = getStorage().get(marshall(key));

        // Force return null on ttl expiration (but before cleanup thread run)
        long ttlMillis = replicatedRecord == null ? 0 : replicatedRecord.getTtlMillis();
        if (ttlMillis > 0 && Clock.currentTimeMillis() - replicatedRecord.getUpdateTime() >= ttlMillis) {
            replicatedRecord = null;
        }

        Object value = replicatedRecord == null ? null : unmarshall(replicatedRecord.getValue());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementGetsNanos(Timer.nanosElapsed(startNanos));
        }
        return value;
    }

    @Override
    public Object put(Object key, Object value) {
        isNotNull(key, "key");
        isNotNull(value, "value");
        return put(key, value, 0, TimeUnit.MILLISECONDS, true);
    }

    @Override
    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit, boolean incrementHits) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        Object old = put(storage, key, value, ttl, timeUnit, incrementHits);
        storage.incrementVersion();
        return old;
    }

    @Override
    public Object putWithVersion(Object key, Object value, long ttl, TimeUnit timeUnit, boolean incrementHits, long version) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        Object old = put(storage, key, value, ttl, timeUnit, incrementHits);
        storage.setVersion(version);
        return old;
    }

    @SuppressWarnings("unchecked")
    private Object put(InternalReplicatedMapStorage<K, V> storage, Object key, Object value,
                       long ttl, TimeUnit timeUnit, boolean incrementHits) {
        isNotNull(key, "key");
        isNotNull(value, "value");
        isNotNull(timeUnit, "timeUnit");
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl must be a positive integer");
        }
        long startNanos = Timer.nanos();
        V oldValue = null;
        K marshalledKey = (K) marshall(key);
        V marshalledValue = (V) marshall(value);
        long ttlMillis = ttl == 0 ? 0 : timeUnit.toMillis(ttl);
        ReplicatedRecord<K, V> old = storage.get(marshalledKey);
        ReplicatedRecord<K, V> record;
        if (old == null) {
            record = buildReplicatedRecord(marshalledKey, marshalledValue, ttlMillis);
            storage.put(marshalledKey, record);
        } else {
            oldValue = old.getValueInternal();
            if (incrementHits) {
                old.setValue(marshalledValue, ttlMillis);
            } else {
                old.setValueInternal(marshalledValue, ttlMillis);
            }
            storage.put(marshalledKey, old);
        }
        if (ttlMillis > 0) {
            scheduleTtlEntry(ttlMillis, marshalledKey, marshalledValue);
        } else {
            cancelTtlEntry(marshalledKey);
        }
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementPutsNanos(Timer.nanosElapsed(startNanos));
        }
        return oldValue;
    }

    @Override
    public boolean containsKey(Object key) {
        isNotNull(key, "key");
        getStats().incrementOtherOperations();
        return containsKeyAndValue(key);
    }

    // IMPORTANT >> Increments hit counter
    private boolean containsKeyAndValue(Object key) {
        ReplicatedRecord<K, V> replicatedRecord = getStorage().get(marshall(key));
        return replicatedRecord != null && replicatedRecord.getValue() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        isNotNull(value, "value");
        getStats().incrementOtherOperations();
        Object v = unmarshall(value);
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : getStorage().entrySet()) {
            V entryValue = entry.getValue().getValue();
            if (v == entryValue || (entryValue != null && unmarshall(entryValue).equals(v))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set keySet(boolean lazy) {
        getStats().incrementOtherOperations();

        if (lazy) {
            // Lazy evaluation to prevent to much copying
            return new LazySet<>(new KeySetIteratorFactory<>(this), getStorage());
        }
        return getStorage().keySet();
    }

    @Override
    public Collection values(boolean lazy) {
        getStats().incrementOtherOperations();

        if (lazy) {
            // Lazy evaluation to prevent to much copying
            return new LazyCollection<>(new ValuesIteratorFactory<>(this), getStorage());
        }
        return getStorage().values();
    }

    @Override
    public Collection values(Comparator comparator) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        List<Object> values = new ArrayList<>(storage.size());
        for (ReplicatedRecord<K, V> record : storage.values()) {
            values.add(unmarshall(record.getValue()));
        }
        getStats().incrementOtherOperations();
        return values;
    }

    @Override
    public Set entrySet(boolean lazy) {
        getStats().incrementOtherOperations();

        if (lazy) {
            // Lazy evaluation to prevent to much copying
            return new LazySet<>(new EntrySetIteratorFactory<>(this), getStorage());
        }
        return getStorage().entrySet();
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Object key) {
        isNotNull(key, "key");
        return getStorage().get(marshall(key));
    }

    @Override
    public boolean isEmpty() {
        getStats().incrementOtherOperations();
        return getStorage().isEmpty();
    }

    @Override
    public int size() {
        getStats().incrementOtherOperations();
        return getStorage().size();
    }

    @Override
    public void clear() {
        clearInternal().incrementVersion();
    }

    @Override
    public void clearWithVersion(long version) {
        clearInternal().setVersion(version);
    }

    @Override
    public void reset() {
        destroy();
    }

    @Override
    public Iterator recordIterator() {
        return new RecordIterator(getStorage().entrySet().iterator());
    }

    public void putRecords(Collection<RecordMigrationInfo> records, long version) {
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        for (RecordMigrationInfo record : records) {
            putRecord(storage, record);
        }
        storage.syncVersion(version);
    }

    @SuppressWarnings("unchecked")
    private void putRecord(InternalReplicatedMapStorage<K, V> storage, RecordMigrationInfo record) {
        K key = (K) marshall(record.getKey());
        V value = (V) marshall(record.getValue());
        ReplicatedRecord<K, V> newRecord = buildReplicatedRecord(key, value, record.getTtl());
        newRecord.setHits(record.getHits());
        newRecord.setCreationTime(record.getCreationTime());
        newRecord.setLastAccessTime(record.getLastAccessTime());
        newRecord.setUpdateTime(record.getLastUpdateTime());
        storage.put(key, newRecord);
        if (record.getTtl() > 0) {
            scheduleTtlEntry(record.getTtl(), key, value);
        }
    }

    private ReplicatedRecord<K, V> buildReplicatedRecord(K key, V value, long ttlMillis) {
        return new ReplicatedRecord<>(key, value, ttlMillis);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean merge(ReplicatedMapMergeTypes<Object, Object> mergingEntry,
                         SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy) {
        mergingEntry = (ReplicatedMapMergeTypes<Object, Object>)
            serializationService.getManagedContext().initialize(mergingEntry);
        mergePolicy = (SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object>)
            serializationService.getManagedContext().initialize(mergePolicy);

        K marshalledKey = (K) marshall(mergingEntry.getRawKey());
        InternalReplicatedMapStorage<K, V> storage = getStorage();
        ReplicatedRecord<K, V> record = storage.get(marshalledKey);
        if (record == null) {
            V newValue = (V) mergePolicy.merge(mergingEntry, null);
            if (newValue == null) {
                return false;
            }
            record = buildReplicatedRecord(marshalledKey, newValue, 0);
            storage.put(marshalledKey, record);
            storage.incrementVersion();
            Data dataKey = serializationService.toData(marshalledKey);
            Data dataValue = serializationService.toData(newValue);
            VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getRawValue(), getVersion());
            sendReplicationOperation(false, name, dataKey, dataValue, record.getTtlMillis(), responsePair);
        } else {
            ReplicatedMapMergeTypes<Object, Object> existingEntry = createMergingEntry(serializationService, record);
            V newValue = (V) mergePolicy.merge(mergingEntry, existingEntry);
            if (newValue == null) {
                storage.remove(marshalledKey, record);
                storage.incrementVersion();
                Data dataKey = serializationService.toData(marshalledKey);
                VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getRawValue(), getVersion());
                sendReplicationOperation(true, name, dataKey, null, record.getTtlMillis(), responsePair);
                return false;
            }
            record.setValueInternal(newValue, record.getTtlMillis());
            storage.incrementVersion();
            Data dataKey = serializationService.toData(marshalledKey);
            Data dataValue = serializationService.toData(newValue);
            VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getRawValue(), getVersion());
            sendReplicationOperation(false, name, dataKey, dataValue, record.getTtlMillis(), responsePair);
        }
        return true;
    }

    private void sendReplicationOperation(boolean isRemove, String name, Data key, Data value, long ttl,
                                          VersionResponsePair response) {
        Collection<Member> members = nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        for (Member member : members) {
            invoke(isRemove, member.getAddress(), name, key, value, ttl, response);
        }
    }

    private void invoke(boolean isRemove, Address address, String name, Data key, Data value, long ttl,
                        VersionResponsePair response) {
        OperationService operationService = nodeEngine.getOperationService();
        ReplicateUpdateOperation updateOperation = new ReplicateUpdateOperation(name, key, value, ttl,
                response, isRemove, nodeEngine.getThisAddress());
        updateOperation.setPartitionId(partitionId);
        updateOperation.setValidateTarget(false);
        operationService.invokeOnTarget(SERVICE_NAME, updateOperation, address);
    }

    private final class RecordIterator implements Iterator<ReplicatedRecord<K, V>> {

        private final Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator;

        private Map.Entry<K, ReplicatedRecord<K, V>> entry;

        private RecordIterator(Iterator<Map.Entry<K, ReplicatedRecord<K, V>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                entry = iterator.next();
                if (testEntry(entry)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public ReplicatedRecord<K, V> next() {
            Map.Entry<K, ReplicatedRecord<K, V>> entry = this.entry;
            ReplicatedRecord<K, V> record = entry != null ? entry.getValue() : null;
            while (entry == null) {
                entry = findNextEntry();
                Object key = entry.getKey();
                record = entry.getValue();
                Object value = record != null ? record.getValue() : null;
                if (key != null && value != null) {
                    break;
                }
            }
            this.entry = null;
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Lazy structures are not modifiable");
        }

        private boolean testEntry(Map.Entry<K, ReplicatedRecord<K, V>> entry) {
            return entry.getKey() != null && entry.getValue() != null;
        }

        private Map.Entry<K, ReplicatedRecord<K, V>> findNextEntry() {
            Map.Entry<K, ReplicatedRecord<K, V>> entry = null;
            while (iterator.hasNext()) {
                entry = iterator.next();
                if (testEntry(entry)) {
                    break;
                }
                entry = null;
            }
            if (entry == null) {
                throw new NoSuchElementException();
            }
            return entry;
        }
    }
}
