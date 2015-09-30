/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.operation.ReplicateUpdateOperation;
import com.hazelcast.replicatedmap.impl.operation.VersionResponsePair;
import com.hazelcast.replicatedmap.merge.ReplicatedMapMergePolicy;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * This is the base class for all {@link ReplicatedRecordStore} implementations
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class AbstractReplicatedRecordStore<K, V> extends AbstractBaseReplicatedRecordStore<K, V> {

    // entries are not removed on replicatedMap.remove() as it would reset a vector clock and we wouldn't be able to
    // order subsequent events related to the entry. a tombstone is created instead. this constant says how long we
    // keep the tombstone alive. if there is no event in this period then the tombstone is removed.
    public static final int TOMBSTONE_REMOVAL_PERIOD_MS = 5 * 60 * 1000;

    public AbstractReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {

        super(name, replicatedMapService, partitionId);
    }

    @Override
    public void removeTombstone(Object key) {
        isNotNull(key, "key");
        K marshalledKey = (K) marshall(key);
        ReplicatedRecord<K, V> current = storage.get(marshalledKey);
        if (current == null || current.getValueInternal() != null) {
            return;
        }
        storage.remove(marshalledKey, current);
    }

    @Override
    public Object remove(Object key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        V oldValue;
        K marshalledKey = (K) marshall(key);
        final ReplicatedRecord current = storage.get(marshalledKey);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = (V) current.getValueInternal();
            if (oldValue != null) {
                current.setValue(null, TOMBSTONE_REMOVAL_PERIOD_MS);
                storage.incrementVersion();
                scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, marshalledKey, null);
            }
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementRemoves(Clock.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public void evict(Object key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        V oldValue;
        K marshalledKey = (K) marshall(key);
        final ReplicatedRecord current = storage.get(marshalledKey);
        if (current == null) {
            oldValue = null;
        } else {
            oldValue = (V) current.getValueInternal();
            if (oldValue != null) {
                current.setValueInternal(null, TOMBSTONE_REMOVAL_PERIOD_MS);
                scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, marshalledKey, null);
            }
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null, EntryEventType.EVICTED);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementRemoves(Clock.currentTimeMillis() - time);
        }
    }

    @Override
    public Object get(Object key) {
        isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        ReplicatedRecord replicatedRecord = storage.get(marshall(key));

        // Force return null on ttl expiration (but before cleanup thread run)
        long ttlMillis = replicatedRecord == null ? 0 : replicatedRecord.getTtlMillis();
        if (ttlMillis > 0 && Clock.currentTimeMillis() - replicatedRecord.getUpdateTime() >= ttlMillis) {
            replicatedRecord = null;
        }

        Object value = replicatedRecord == null ? null : unmarshall(replicatedRecord.getValue());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementGets(Clock.currentTimeMillis() - time);
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
        isNotNull(key, "key");
        isNotNull(value, "value");
        isNotNull(timeUnit, "timeUnit");
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl must be a positive integer");
        }
        long time = Clock.currentTimeMillis();
        V oldValue = null;
        K marshalledKey = (K) marshall(key);
        V marshalledValue = (V) marshall(value);
        final long ttlMillis = ttl == 0 ? 0 : timeUnit.toMillis(ttl);
        final ReplicatedRecord old = storage.get(marshalledKey);
        ReplicatedRecord<K, V> record = old;
        if (old == null) {
            record = buildReplicatedRecord(marshalledKey, marshalledValue, ttlMillis);
            storage.put(marshalledKey, record);
        } else {
            oldValue = (V) old.getValueInternal();
            if (incrementHits) {
                storage.get(marshalledKey).setValue(marshalledValue, ttlMillis);
            } else {
                storage.get(marshalledKey).setValueInternal(marshalledValue, ttlMillis);
            }
            storage.incrementVersion();
        }
        if (ttlMillis > 0) {
            scheduleTtlEntry(ttlMillis, marshalledKey, marshalledValue);
        } else {
            cancelTtlEntry(marshalledKey);
        }
        Object unmarshalledOldValue = unmarshall(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, value);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            getStats().incrementPuts(Clock.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public boolean containsKey(Object key) {
        isNotNull(key, "key");
        getStats().incrementOtherOperations();
        return containsKeyAndValue(key);
    }

    // IMPORTANT >> Increments hit counter
    private boolean containsKeyAndValue(Object key) {
        ReplicatedRecord replicatedRecord = storage.get(marshall(key));
        return replicatedRecord != null && replicatedRecord.getValue() != null;
    }

    @Override
    public boolean containsValue(Object value) {
        isNotNull(value, "value");
        getStats().incrementOtherOperations();
        Object v = unmarshall(value);
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : storage.entrySet()) {
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
            return new LazySet<K, V, K>(new KeySetIteratorFactory<K, V>(this), storage);
        }
        return storage.keySet();
    }

    @Override
    public Collection values(boolean lazy) {
        getStats().incrementOtherOperations();

        if (lazy) {
            // Lazy evaluation to prevent to much copying
            return new LazyCollection<K, V>(new ValuesIteratorFactory<K, V>(this), storage);
        }
        return storage.values();
    }

    @Override
    public Collection values(Comparator comparator) {
        List values = new ArrayList(storage.size());
        for (ReplicatedRecord record : storage.values()) {
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
            return new LazySet<K, V, Map.Entry<K, V>>(new EntrySetIteratorFactory<K, V>(this), storage);
        }
        return storage.entrySet();
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Object key) {
        isNotNull(key, "key");
        return storage.get(marshall(key));
    }

    @Override
    public boolean isEmpty() {
        getStats().incrementOtherOperations();
        return storage.isEmpty();
    }

    @Override
    public int size() {
        getStats().incrementOtherOperations();
        return storage.size();
    }

    @Override
    public void clear() {
        storage.clear();
        getStats().incrementOtherOperations();
    }

    @Override
    public void reset() {
        storage.reset();
    }

    @Override
    public Iterator recordIterator() {
        return new RecordIterator(storage.entrySet().iterator());
    }

    @Override
    public void putRecord(RecordMigrationInfo record) {
        K key = (K) marshall(record.getKey());
        V value = (V) marshall(record.getValue());
        storage.putInternal(key, buildReplicatedRecord(key, value, record.getTtl()));
    }

    private ReplicatedRecord buildReplicatedRecord(Object key, Object value, long ttlMillis) {
        int partitionId = partitionService.getPartitionId(key);
        return new ReplicatedRecord(key, value, ttlMillis, partitionId);
    }


    @Override
    public boolean merge(Object key, ReplicatedMapEntryView mergingEntry, ReplicatedMapMergePolicy policy) {
        Object marshalledKey = marshall(key);
        ReplicatedRecord<K, V> record = storage.get(marshalledKey);
        Object newValue;
        if (record == null) {
            ReplicatedMapEntryView nullEntryView = new ReplicatedMapEntryView(unmarshall(key), null);
            newValue = policy.merge(getName(), mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            record = buildReplicatedRecord(marshalledKey, newValue, 0);
            storage.put((K) marshalledKey, record);
            Data dataKey = serializationService.toData(marshalledKey);
            Data dataValue = serializationService.toData(newValue);
            VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getValue(), getVersion());
            sendReplicationOperation(false, getName(), dataKey, dataValue, record.getTtlMillis(), responsePair);
        } else {
            Object oldValue = record.getValueInternal();
            ReplicatedMapEntryView existingEntry = new ReplicatedMapEntryView(unmarshall(key), unmarshall(oldValue));
            existingEntry.setCreationTime(record.getCreationTime());
            existingEntry.setLastUpdateTime(record.getUpdateTime());
            existingEntry.setLastAccessTime(record.getLastAccessTime());
            existingEntry.setHits(record.getHits());
            existingEntry.setTtl(record.getTtlMillis());
            newValue = policy.merge(getName(), mergingEntry, existingEntry);
            if (newValue == null) {
                record.setValue(null, TOMBSTONE_REMOVAL_PERIOD_MS);
                storage.incrementVersion();
                scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, (K) marshalledKey, null);
                Data dataKey = serializationService.toData(marshalledKey);
                VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getValue(), getVersion());
                sendReplicationOperation(true, getName(), dataKey, null, record.getTtlMillis(), responsePair);
                return false;
            }
            storage.incrementVersion();
            record.setValueInternal((V) newValue, record.getTtlMillis());
            Data dataKey = serializationService.toData(marshalledKey);
            Data dataValue = serializationService.toData(newValue);
            VersionResponsePair responsePair = new VersionResponsePair(mergingEntry.getValue(), getVersion());
            sendReplicationOperation(false, getName(), dataKey, dataValue, record.getTtlMillis(), responsePair);

        }
        return true;
    }

    protected void sendReplicationOperation(final boolean isRemove, String name, Data key, Data value, long ttl,
                                            VersionResponsePair response) {
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            invoke(isRemove, member.getAddress(), name, key, value, ttl, response);
        }
    }

    private void invoke(boolean isRemove, Address address, String name, Data key, Data value, long ttl,
                        VersionResponsePair response) {
        OperationService operationService = nodeEngine.getOperationService();
        ReplicateUpdateOperation updateOperation = new ReplicateUpdateOperation(name, key, value, ttl,
                response, isRemove);
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
            Object key = entry != null ? entry.getKey() : null;
            Object value = entry != null && entry.getValue() != null ? entry.getValue().getValue() : null;
            ReplicatedRecord<K, V> record = entry.getValue();
            while (entry == null) {
                entry = findNextEntry();
                key = entry.getKey();
                record = entry.getValue();
                value = record != null ? record.getValue() : null;
                if (key != null && value != null) {
                    break;
                }
            }
            this.entry = null;
            if (key == null || value == null) {
                throw new NoSuchElementException();
            }
            return record;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Lazy structures are not modifiable");
        }

        private boolean testEntry(Map.Entry<K, ReplicatedRecord<K, V>> entry) {
            return entry.getKey() != null && entry.getValue() != null && !entry.getValue().isTombstone();
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
