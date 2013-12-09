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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.operation.ReplicatedMapInitChunkOperation;
import com.hazelcast.spi.*;
import com.hazelcast.util.executor.NamedThreadFactory;
import com.hazelcast.util.nonblocking.NonBlockingHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractReplicatedRecordStorage<K, V> implements ReplicatedRecordStore {

    protected final ConcurrentMap<K, ReplicatedRecord<K, V>> storage = new NonBlockingHashMap<K, ReplicatedRecord<K, V>>();
    private final AtomicInteger initialFillupThreadNumber = new AtomicInteger(0);

    private final String name;
    private final Member localMember;
    private final int localMemberHash;
    private final NodeEngine nodeEngine;
    private final EventService eventService;

    private final ExecutorService executorService;

    private final ReplicatedMapService replicatedMapService;
    private final ReplicatedMapConfig replicatedMapConfig;

    private final ScheduledFuture<?> cleanerFuture;
    private final Object[] mutexes;

    public AbstractReplicatedRecordStorage(String name, NodeEngine nodeEngine,
                                           CleanerRegistrator cleanerRegistrator,
                                           ReplicatedMapService replicatedMapService) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localMember = nodeEngine.getLocalMember();
        this.localMemberHash = localMember.getUuid().hashCode();
        this.eventService = nodeEngine.getEventService();
        this.replicatedMapService = replicatedMapService;
        this.replicatedMapConfig = replicatedMapService.getReplicatedMapConfig(name);
        this.executorService = getExecutorService(replicatedMapConfig);

        this.mutexes = new Object[replicatedMapConfig.getConcurrencyLevel()];
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }

        this.cleanerFuture = cleanerRegistrator.registerCleaner(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object remove(Object key) {
        V old;
        Object marshalledKey = marshallKey(key);
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord current = storage.get(marshalledKey);
            final Vector vector;
            if (current == null) {
                old = null;
            } else {
                vector = current.getVector();
                old = (V) current.getValue();
                current.setValue(null, 0);
                incrementClock(vector);
                publishReplicatedMessage(new ReplicationMessage(name, key, null, vector, localMember, localMemberHash));
            }
        }
        return unmarshallValue(old);
    }

    @Override
    public Object get(Object key) {
        ReplicatedRecord replicatedRecord = storage.get(marshallKey(key));
        return replicatedRecord == null ? null : unmarshallValue(replicatedRecord.getValue());
    }

    @Override
    public Object put(Object key, Object value) {
        V oldValue = null;
        K marshalledKey = (K) marshallKey(key);
        V marshalledValue = (V) marshallValue(value);
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord old = storage.get(marshalledKey);
            final Vector vector;
            if (old == null) {
                vector = new Vector();
                ReplicatedRecord<K, V> record = new ReplicatedRecord(marshalledKey, marshalledValue, vector, localMemberHash);
                storage.put(marshalledKey, record);
            } else {
                oldValue = (V) old.getValue();
                vector = old.getVector();

                storage.get(marshalledKey).setValue(marshalledValue, localMemberHash);
            }
            incrementClock(vector);
            publishReplicatedMessage(new ReplicationMessage(name, key, value, vector, localMember, localMemberHash));
        }
        return unmarshallValue(oldValue);
    }

    @Override
    public boolean containsKey(Object key) {
        return storage.containsKey(marshallKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : storage.entrySet()) {
            V entryValue = entry.getValue().getValue();
            if (value == entryValue
                    || (entryValue != null && unmarshallValue(entryValue).equals(value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set keySet() {
        Set keySet = new HashSet(storage.size());
        for (K key : storage.keySet()) {
            keySet.add(unmarshallKey(key));
        }
        return keySet;
    }

    @Override
    public Collection values() {
        List values = new ArrayList(storage.size());
        for (ReplicatedRecord record : storage.values()) {
            values.add(unmarshallValue(record.getValue()));
        }
        return values;
    }

    @Override
    public Set entrySet() {
        Set entrySet = new HashSet(storage.size());
        for (Map.Entry entry : storage.entrySet()) {
            entrySet.add(new AbstractMap.SimpleEntry(unmarshallKey(entry.getKey()), unmarshallValue(entry.getValue())));
        }
        return entrySet;
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Object key) {
        return storage.get(marshallKey(key));
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public int size() {
        return storage.size();
    }

    @Override
    public void clear() {
        //TODO distribute clear
        storage.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AbstractReplicatedRecordStorage) {
            return storage.equals(((AbstractReplicatedRecordStorage) o).storage);
        }
        return storage.equals(o);
    }

    @Override
    public int hashCode() {
        return storage.hashCode();
    }

    @Override
    public ReplicatedMapService getReplicatedMapService() {
        return replicatedMapService;
    }

    @Override
    public void destroy() {
        if (cleanerFuture.isCancelled()) {
            return;
        }
        cleanerFuture.cancel(true);
        executorService.shutdownNow();
        storage.clear();
        replicatedMapService.destroyDistributedObject(getName());
    }

    public Set<ReplicatedRecord> getRecords() {
        return new HashSet<ReplicatedRecord>(storage.values());
    }

    public void queueInitialFillup(Address callerAddress, int chunkSize) {
        String threadName = "ReplicatedMap-" + name + "-Fillup-" + initialFillupThreadNumber.getAndIncrement();
        Thread fillupThread = new Thread(new RemoteFillupTask(callerAddress, chunkSize), threadName);
        fillupThread.setDaemon(true);
        fillupThread.start();
    }

    public void queueUpdateMessage(final ReplicationMessage update) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                processUpdateMessage(update);
            }
        });
    }

    protected abstract Object unmarshallKey(Object key);

    protected abstract Object unmarshallValue(Object value);

    protected abstract Object marshallKey(Object key);

    protected abstract Object marshallValue(Object value);

    protected void publishReplicatedMessage(IdentifiedDataSerializable message) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(ReplicatedMapService.SERVICE_NAME, ReplicatedMapService.EVENT_TOPIC_NAME);
        eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, message, name.hashCode());
    }

    protected void incrementClock(Vector vector) {
        final AtomicInteger clock = vector.clocks.get(localMember);
        if (clock != null) {
            clock.incrementAndGet();
        } else {
            vector.clocks.put(localMember, new AtomicInteger(1));
        }
    }

    protected Object getMutex(final Object key) {
        return mutexes[key.hashCode() != Integer.MIN_VALUE ? Math.abs(key.hashCode()) % mutexes.length : 0];
    }

    private void processUpdateMessage(ReplicationMessage update) {
        if (localMember.equals(update.getOrigin())) {
            return;
        }
        K marshalledKey = (K) marshallKey(update.getKey());
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord<K, V> localEntry = storage.get(marshalledKey);
            if (localEntry == null) {
                if (!update.isRemove()) {
                    V marshalledValue = (V) marshallValue(update.getValue());
                    Vector vector = update.getVector();
                    int updateHash = update.getUpdateHash();
                    storage.put(marshalledKey, new ReplicatedRecord<K, V>(marshalledKey, marshalledValue, vector, updateHash));
                }
            } else {
                final Vector currentVector = localEntry.getVector();
                final Vector updateVector = update.getVector();
                if (Vector.happenedBefore(updateVector, currentVector)) {
                    // ignore the update. This is an old update
                } else if (Vector.happenedBefore(currentVector, updateVector)) {
                    // A new update happened
                    applyTheUpdate(update, localEntry);
                } else {
                    // no preceding among the clocks. Lower hash wins..
                    if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                        applyTheUpdate(update, localEntry);
                    } else {
                        applyVector(updateVector, currentVector);
                        publishReplicatedMessage(new ReplicationMessage(name, update.getKey(), localEntry.getValue(),
                                currentVector, localMember, localEntry.getLatestUpdateHash()));
                    }
                }
            }
        }
    }

    private void applyTheUpdate(ReplicationMessage<K, V> update, ReplicatedRecord<K, V> localEntry) {
        Vector localVector = localEntry.getVector();
        Vector remoteVector = update.getVector();
        localEntry.setValue((V) marshallValue(update.getValue()), update.getUpdateHash());
        applyVector(remoteVector, localVector);
    }

    private void applyVector(Vector update, Vector current) {
        for (Member m : update.clocks.keySet()) {
            final AtomicInteger currentClock = current.clocks.get(m);
            final AtomicInteger updateClock = update.clocks.get(m);
            if (smaller(currentClock, updateClock)) {
                current.clocks.put(m, new AtomicInteger(updateClock.get()));
            }
        }
    }

    private boolean smaller(AtomicInteger int1, AtomicInteger int2) {
        int i1 = int1 == null ? 0 : int1.get();
        int i2 = int2 == null ? 0 : int2.get();
        return i1 < i2;
    }

    private ExecutorService getExecutorService(ReplicatedMapConfig replicatedMapConfig) {
        ExecutorService es = replicatedMapConfig.getReplicatorExecutorService();
        if (es != null) {
            return new WrappedExecutorService(es);
        }
        return Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(ReplicatedMapService.SERVICE_NAME + "." + name + ".replicator"));
    }

    private class RemoteFillupTask implements Runnable {

        private final PartitionService partitionService = nodeEngine.getPartitionService();
        private final OperationService operationService = nodeEngine.getOperationService();
        private final Address callerAddress;
        private final int chunkSize;

        private ReplicatedRecord[] recordCache;
        private int recordCachePos = 0;

        private RemoteFillupTask(Address callerAddress, int chunkSize) {
            this.callerAddress = callerAddress;
            this.chunkSize = chunkSize;
        }

        @Override
        public void run() {
            recordCache = new ReplicatedRecord[chunkSize];
            for (ReplicatedRecord<K, V> replicatedRecord : storage.values()) {
                processReplicatedRecord(replicatedRecord);
            }
            sendChunk();
        }

        private void processReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord) {
            Object key = marshallKey(replicatedRecord.getKey());
            int partitionId = partitionService.getPartitionId(key);
            if (partitionService.getPartitionOwner(partitionId).equals(nodeEngine.getThisAddress())) {
                synchronized (getMutex(key)) {
                    pushReplicatedRecord(replicatedRecord);
                }
            }
        }

        private void pushReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord) {
            if (recordCachePos == chunkSize) {
                sendChunk();
            }

            int hash = replicatedRecord.getLatestUpdateHash();
            Object key = unmarshallKey(replicatedRecord.getKey());
            Object value = unmarshallValue(replicatedRecord.getValue());
            Vector vector = Vector.copyVector(replicatedRecord.getVector());
            recordCache[recordCachePos++] = new ReplicatedRecord(key, value, vector, hash);
        }

        private void sendChunk() {
            if (recordCachePos > 0) {
                Operation operation = new ReplicatedMapInitChunkOperation(name, localMember, recordCache, recordCachePos);
                operationService.send(operation, callerAddress);

                // Reset chunk cache and pos
                recordCache = new ReplicatedRecord[chunkSize];
                recordCachePos = 0;
            }
        }
    }

}
