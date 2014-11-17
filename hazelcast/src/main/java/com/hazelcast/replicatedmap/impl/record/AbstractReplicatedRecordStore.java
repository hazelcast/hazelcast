/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ValidationUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is the base class for all {@link ReplicatedRecordStore} implementations
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class AbstractReplicatedRecordStore<K, V>
        extends AbstractBaseReplicatedRecordStore<K, V> {
    static final String CLEAR_REPLICATION_MAGIC_KEY = ReplicatedMapService.SERVICE_NAME + "$CLEAR$MESSAGE$";

//    entries are not removed on replicatedMap.remove() as it would reset a vector clock and we wouldn't be able to
//    order subsequent events related to the entry. a tombstone is created instead. this constant says how long we
//    keep the tombstone alive. if there is no event in this period then the tombstone is removed.
    static final int TOMBSTONE_REMOVAL_PERIOD_MS = 5 * 60 * 1000;

    public AbstractReplicatedRecordStore(String name, NodeEngine nodeEngine,
                                         ReplicatedMapService replicatedMapService) {

        super(name, nodeEngine, replicatedMapService);
    }

    @Override
    public void removeTombstone(Object key) {
        ValidationUtil.isNotNull(key, "key");
        storage.checkState();
        K marshalledKey = (K) marshallKey(key);
        synchronized (getMutex(marshalledKey)) {
            ReplicatedRecord<K, V> current = storage.get(marshalledKey);
            if (current == null || current.getValue() != null) {
                return;
            }
            storage.remove(marshalledKey, current);
        }
    }

    @Override
    public Object remove(Object key) {
        ValidationUtil.isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue;
        K marshalledKey = (K) marshallKey(key);
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord current = storage.get(marshalledKey);
            final VectorClockTimestamp vectorClockTimestamp;
            if (current == null) {
                oldValue = null;
            } else {
                oldValue = (V) current.getValue();
                if (oldValue != null) {
                    current.setValue(null, localMemberHash, TOMBSTONE_REMOVAL_PERIOD_MS);
                    scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, marshalledKey, null);
                    vectorClockTimestamp = current.incrementVectorClock(localMember);
                    ReplicationMessage message = buildReplicationMessage(key, null, vectorClockTimestamp,
                            TOMBSTONE_REMOVAL_PERIOD_MS);
                    replicationPublisher.publishReplicatedMessage(message);
                }
            }
        }
        Object unmarshalledOldValue = unmarshallValue(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementRemoves(Clock.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public void evict(Object key) {
        ValidationUtil.isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue;
        K marshalledKey = (K) marshallKey(key);
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord current = storage.get(marshalledKey);
            if (current == null) {
                oldValue = null;
            } else {
                oldValue = (V) current.getValue();
                if (oldValue != null) {
                    current.setValue(null, localMemberHash, TOMBSTONE_REMOVAL_PERIOD_MS);
                    scheduleTtlEntry(TOMBSTONE_REMOVAL_PERIOD_MS, marshalledKey, null);
                    current.incrementVectorClock(localMember);
                }
            }
        }
        Object unmarshalledOldValue = unmarshallValue(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null, EntryEventType.EVICTED);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementRemoves(Clock.currentTimeMillis() - time);
        }
    }

    @Override
    public Object get(Object key) {
        ValidationUtil.isNotNull(key, "key");
        long time = Clock.currentTimeMillis();
        storage.checkState();
        ReplicatedRecord replicatedRecord = storage.get(marshallKey(key));

        // Force return null on ttl expiration (but before cleanup thread run)
        long ttlMillis = replicatedRecord == null ? 0 : replicatedRecord.getTtlMillis();
        if (ttlMillis > 0 && Clock.currentTimeMillis() - replicatedRecord.getUpdateTime() >= ttlMillis) {
            replicatedRecord = null;
        }

        Object value = replicatedRecord == null ? null : unmarshallValue(replicatedRecord.getValue());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementGets(Clock.currentTimeMillis() - time);
        }
        return value;
    }

    @Override
    public Object put(Object key, Object value) {
        ValidationUtil.isNotNull(key, "key");
        ValidationUtil.isNotNull(value, "value");
        storage.checkState();
        return put(key, value, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        ValidationUtil.isNotNull(key, "key");
        ValidationUtil.isNotNull(value, "value");
        ValidationUtil.isNotNull(timeUnit, "timeUnit");
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl must be a positive integer");
        }
        long time = Clock.currentTimeMillis();
        storage.checkState();
        V oldValue = null;
        K marshalledKey = (K) marshallKey(key);
        V marshalledValue = (V) marshallValue(value);
        synchronized (getMutex(marshalledKey)) {
            final long ttlMillis = ttl == 0 ? 0 : timeUnit.toMillis(ttl);
            final ReplicatedRecord old = storage.get(marshalledKey);
            ReplicatedRecord<K, V> record = old;
            if (old == null) {
                record = buildReplicatedRecord(marshalledKey, marshalledValue, new VectorClockTimestamp(), ttlMillis);
                storage.put(marshalledKey, record);
            } else {
                oldValue = (V) old.getValue();
                storage.get(marshalledKey).setValue(marshalledValue, localMemberHash, ttlMillis);
            }
            if (ttlMillis > 0) {
                scheduleTtlEntry(ttlMillis, marshalledKey, marshalledValue);
            } else {
                cancelTtlEntry(marshalledKey);
            }

            VectorClockTimestamp vectorClockTimestamp = record.incrementVectorClock(localMember);
            ReplicationMessage message = buildReplicationMessage(key, value, vectorClockTimestamp, ttlMillis);
            replicationPublisher.publishReplicatedMessage(message);
        }
        Object unmarshalledOldValue = unmarshallValue(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, value);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementPuts(Clock.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public boolean containsKey(Object key) {
        ValidationUtil.isNotNull(key, "key");
        storage.checkState();
        mapStats.incrementOtherOperations();
        return storage.containsKey(marshallKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        ValidationUtil.isNotNull(value, "value");
        storage.checkState();
        mapStats.incrementOtherOperations();
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : storage.entrySet()) {
            V entryValue = entry.getValue().getValue();
            if (value == entryValue || (entryValue != null && unmarshallValue(entryValue).equals(value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set keySet() {
        storage.checkState();
        Set keySet = new HashSet(storage.size());
        for (K key : storage.keySet()) {
            keySet.add(unmarshallKey(key));
        }
        mapStats.incrementOtherOperations();
        return keySet;
    }

    @Override
    public Collection values() {
        storage.checkState();
        List values = new ArrayList(storage.size());
        for (ReplicatedRecord record : storage.values()) {
            values.add(unmarshallValue(record.getValue()));
        }
        mapStats.incrementOtherOperations();
        return values;
    }

    @Override
    public Collection values(Comparator comparator) {
        List values = (List) values();
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    public Set entrySet() {
        storage.checkState();
        Set entrySet = new HashSet(storage.size());
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : storage.entrySet()) {
            Object key = unmarshallKey(entry.getKey());
            Object value = unmarshallValue(entry.getValue().getValue());
            entrySet.add(new AbstractMap.SimpleEntry(key, value));
        }
        mapStats.incrementOtherOperations();
        return entrySet;
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Object key) {
        ValidationUtil.isNotNull(key, "key");
        storage.checkState();
        return storage.get(marshallKey(key));
    }

    @Override
    public boolean isEmpty() {
        mapStats.incrementOtherOperations();
        return storage.isEmpty();
    }

    @Override
    public int size() {
        mapStats.incrementOtherOperations();
        return storage.size();
    }

    @Override
    public void clear(boolean distribute, boolean emptyReplicationQueue) {
        storage.checkState();
        if (emptyReplicationQueue) {
            replicationPublisher.emptyReplicationQueue();
        }
        storage.clear();
        if (distribute) {
            replicationPublisher.distributeClear(emptyReplicationQueue);
        }
        mapStats.incrementOtherOperations();
    }

    @Override
    public String addEntryListener(EntryListener listener, Object key) {
        ValidationUtil.isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedEntryEventFilter(marshallKey(key));
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, getName());
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate predicate, Object key) {
        ValidationUtil.isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(marshallKey(key), predicate);
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, getName());
    }

    @Override
    public boolean removeEntryListenerInternal(String id) {
        ValidationUtil.isNotNull(id, "id");
        mapStats.incrementOtherOperations();
        return replicatedMapService.removeEventListener(getName(), id);
    }

    private ReplicationMessage buildReplicationMessage(Object key, Object value, VectorClockTimestamp timestamp, long ttlMillis) {
        return new ReplicationMessage(getName(), key, value, timestamp, localMember, localMemberHash, ttlMillis);
    }

    private ReplicatedRecord buildReplicatedRecord(Object key, Object value, VectorClockTimestamp timestamp, long ttlMillis) {
        return new ReplicatedRecord(key, value, timestamp, localMemberHash, ttlMillis);
    }
}
