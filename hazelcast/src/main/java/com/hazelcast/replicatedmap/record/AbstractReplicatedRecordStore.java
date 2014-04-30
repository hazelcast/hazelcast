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

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.PreReplicationHook;
import com.hazelcast.replicatedmap.ReplicatedMapEvictionProcessor;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.ReplicationChannel;
import com.hazelcast.replicatedmap.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.operation.ReplicatedMapInitChunkOperation;
import com.hazelcast.replicatedmap.operation.ReplicatedMapPostJoinOperation;
import com.hazelcast.replicatedmap.operation.ReplicatedMapPostJoinOperation.MemberMapPair;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ValidationUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduleType;
import com.hazelcast.util.scheduler.ScheduledEntry;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractReplicatedRecordStore<K, V>
        implements ReplicatedRecordStore, InitializingObject, ReplicationChannel {

    private static final int MAX_MESSAGE_CACHE_SIZE = 1000; // TODO: this constant may be configurable...

    protected final ConcurrentMap<K, ReplicatedRecord<K, V>> storage = new ConcurrentHashMap<K, ReplicatedRecord<K, V>>();

    private final LocalReplicatedMapStatsImpl mapStats = new LocalReplicatedMapStatsImpl();

    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final Lock waitForLoadedLock = new ReentrantLock();
    private final Condition waitForLoadedCondition = waitForLoadedLock.newCondition();
    private final Random memberRandomizer = new Random();

    private final List<ReplicationMessage> replicationMessageCache = new ArrayList<ReplicationMessage>();
    private final Lock replicationMessageCacheLock = new ReentrantLock();

    private final String name;
    private final Member localMember;
    private final int localMemberHash;
    private final NodeEngine nodeEngine;
    private final EventService eventService;
    private final ExecutionService executionService;

    private final ScheduledExecutorService executorService;
    private final EntryTaskScheduler ttlEvictionScheduler;

    private final ReplicatedMapService replicatedMapService;
    private final ReplicatedMapConfig replicatedMapConfig;

    private final ScheduledFuture<?> cleanerFuture;
    private final Object[] mutexes;

    private final boolean allowReplicationHooks;
    private volatile PreReplicationHook preReplicationHook;

    public AbstractReplicatedRecordStore(String name, NodeEngine nodeEngine, CleanerRegistrator cleanerRegistrator,
                                         ReplicatedMapService replicatedMapService) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localMember = nodeEngine.getLocalMember();
        this.localMemberHash = localMember.getUuid().hashCode();
        this.eventService = nodeEngine.getEventService();
        this.executionService = nodeEngine.getExecutionService();
        this.replicatedMapService = replicatedMapService;
        this.replicatedMapConfig = replicatedMapService.getReplicatedMapConfig(name);
        this.executorService = getExecutorService(nodeEngine, replicatedMapConfig);
        this.ttlEvictionScheduler = EntryTaskSchedulerFactory
                .newScheduler(nodeEngine.getExecutionService().getDefaultScheduledExecutor(),
                        new ReplicatedMapEvictionProcessor(nodeEngine, replicatedMapService, name), ScheduleType.POSTPONE);

        this.mutexes = new Object[replicatedMapConfig.getConcurrencyLevel()];
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }

        this.cleanerFuture = cleanerRegistrator.registerCleaner(this);

        this.allowReplicationHooks = Boolean.parseBoolean(System.getProperty("hazelcast.repmap.hooks.allowed", "false"));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object remove(Object key) {
        ValidationUtil.isNotNull(key, "key");
        long time = System.currentTimeMillis();
        checkState();
        V oldValue;
        K marshalledKey = (K) marshallKey(key);
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord current = storage.get(marshalledKey);
            final VectorClock vectorClock;
            if (current == null) {
                oldValue = null;
            } else {
                vectorClock = current.getVectorClock();
                oldValue = (V) current.getValue();
                current.setValue(null, 0, -1);
                incrementClock(vectorClock);
                publishReplicatedMessage(new ReplicationMessage(name, key, null, vectorClock, localMember, localMemberHash, -1));
            }
            cancelTtlEntry(marshalledKey);
        }
        Object unmarshalledOldValue = unmarshallValue(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, null);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementRemoves(System.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public Object get(Object key) {
        ValidationUtil.isNotNull(key, "key");
        long time = System.currentTimeMillis();
        checkState();
        ReplicatedRecord replicatedRecord = storage.get(marshallKey(key));

        // Force return null on ttl expiration (but before cleanup thread run)
        long ttlMillis = replicatedRecord == null ? 0 : replicatedRecord.getTtlMillis();
        if (ttlMillis > 0 && System.currentTimeMillis() - replicatedRecord.getUpdateTime() > ttlMillis) {
            replicatedRecord = null;
        }

        Object value = replicatedRecord == null ? null : unmarshallValue(replicatedRecord.getValue());
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementGets(System.currentTimeMillis() - time);
        }
        return value;
    }

    @Override
    public Object put(Object key, Object value) {
        ValidationUtil.isNotNull(key, "key");
        ValidationUtil.isNotNull(value, "value");
        checkState();
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
        long time = System.currentTimeMillis();
        checkState();
        V oldValue = null;
        K marshalledKey = (K) marshallKey(key);
        V marshalledValue = (V) marshallValue(value);
        synchronized (getMutex(marshalledKey)) {
            final long ttlMillis = ttl == 0 ? 0 : timeUnit.toMillis(ttl);
            final ReplicatedRecord old = storage.get(marshalledKey);
            final VectorClock vectorClock;
            if (old == null) {
                vectorClock = new VectorClock();
                ReplicatedRecord<K, V> record = new ReplicatedRecord(marshalledKey, marshalledValue, vectorClock, localMemberHash,
                        ttlMillis);
                storage.put(marshalledKey, record);
            } else {
                oldValue = (V) old.getValue();
                vectorClock = old.getVectorClock();
                storage.get(marshalledKey).setValue(marshalledValue, localMemberHash, ttlMillis);
            }
            if (ttlMillis > 0) {
                scheduleTtlEntry(ttlMillis, marshalledKey, null);
            } else {
                cancelTtlEntry(marshalledKey);
            }

            incrementClock(vectorClock);
            publishReplicatedMessage(
                    new ReplicationMessage(name, key, value, vectorClock, localMember, localMemberHash, ttlMillis));
        }
        Object unmarshalledOldValue = unmarshallValue(oldValue);
        fireEntryListenerEvent(key, unmarshalledOldValue, value);
        if (replicatedMapConfig.isStatisticsEnabled()) {
            mapStats.incrementPuts(System.currentTimeMillis() - time);
        }
        return unmarshalledOldValue;
    }

    @Override
    public boolean containsKey(Object key) {
        ValidationUtil.isNotNull(key, "key");
        checkState();
        mapStats.incrementOtherOperations();
        return storage.containsKey(marshallKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        ValidationUtil.isNotNull(value, "value");
        checkState();
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
        checkState();
        Set keySet = new HashSet(storage.size());
        for (K key : storage.keySet()) {
            keySet.add(unmarshallKey(key));
        }
        mapStats.incrementOtherOperations();
        return keySet;
    }

    @Override
    public Collection values() {
        checkState();
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
        checkState();
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
        checkState();
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
    public void clear() {
        throw new UnsupportedOperationException("clear is not supported on ReplicatedMap");
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AbstractReplicatedRecordStore) {
            return storage.equals(((AbstractReplicatedRecordStore) o).storage);
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

    @Override
    public void publishReplicatedMessage(ReplicationMessage message) {
        if (replicatedMapConfig.getReplicationDelayMillis() == 0) {
            distributeReplicationMessage(message, false);
        } else {
            replicationMessageCacheLock.lock();
            try {
                replicationMessageCache.add(message);
                if (replicationMessageCache.size() == 1) {
                    executorService.schedule(new ReplicationCachedSenderTask(), replicatedMapConfig.getReplicationDelayMillis(),
                            TimeUnit.MILLISECONDS);
                } else {
                    if (replicationMessageCache.size() > MAX_MESSAGE_CACHE_SIZE) {
                        processMessageCache();
                    }
                }
            } finally {
                replicationMessageCacheLock.unlock();
            }
        }
    }

    @Override
    public String addEntryListener(EntryListener listener, Object key) {
        ValidationUtil.isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedEntryEventFilter(marshallKey(key));
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, name);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate predicate, Object key) {
        ValidationUtil.isNotNull(listener, "listener");
        EventFilter eventFilter = new ReplicatedQueryEventFilter(marshallKey(key), predicate);
        mapStats.incrementOtherOperations();
        return replicatedMapService.addEventListener(listener, eventFilter, name);
    }

    @Override
    public boolean removeEntryListenerInternal(String id) {
        ValidationUtil.isNotNull(id, "id");
        mapStats.incrementOtherOperations();
        return replicatedMapService.removeEventListener(name, id);
    }

    @Override
    public void initialize() {
        initializeListeners();

        List<MemberImpl> members = new ArrayList<MemberImpl>(nodeEngine.getClusterService().getMemberList());
        members.remove(localMember);
        if (members.size() == 0) {
            loaded.set(true);
        } else {
            sendInitialFillupRequest(members);
        }
    }

    public LocalReplicatedMapStats createReplicatedMapStats() {
        LocalReplicatedMapStatsImpl stats = mapStats;
        stats.setOwnedEntryCount(storage.size());

        List<ReplicatedRecord<K, V>> records = new ArrayList<ReplicatedRecord<K, V>>(storage.values());

        long hits = 0;
        for (ReplicatedRecord<K, V> record : records) {
            stats.setLastAccessTime(record.getLastAccessTime());
            stats.setLastUpdateTime(record.getUpdateTime());
            hits += record.getHits();
        }
        stats.setHits(hits);
        return stats;
    }

    public LocalReplicatedMapStatsImpl getReplicatedMapStats() {
        return mapStats;
    }

    public Set<ReplicatedRecord> getRecords() {
        checkState();
        return new HashSet<ReplicatedRecord>(storage.values());
    }

    public void queueInitialFillup(Address callerAddress, int chunkSize) {
        executionService.execute("hz:replicated-map", new RemoteFillupTask(callerAddress, chunkSize));
    }

    public void queueUpdateMessage(final ReplicationMessage update) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                processUpdateMessage(update);
            }
        });
    }

    public void queueUpdateMessages(final MultiReplicationMessage updates) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                for (ReplicationMessage update : updates.getReplicationMessages()) {
                    processUpdateMessage(update);
                }
            }
        });
    }

    public void finalChunkReceived() {
        loaded.set(true);
        waitForLoadedLock.lock();
        try {
            waitForLoadedCondition.signalAll();
        } finally {
            waitForLoadedLock.unlock();
        }
    }

    public void retryWithDifferentReplicationNode(Member member) {
        List<MemberImpl> members = new ArrayList<MemberImpl>(nodeEngine.getClusterService().getMemberList());
        members.remove(member);

        // If there are less than two members there is not other possible candidate to replicate from
        if (members.size() < 2) {
            return;
        }
        sendInitialFillupRequest(members);
    }

    private void sendInitialFillupRequest(List<MemberImpl> members) {
        if (members.size() == 0) {
            return;
        }
        int randomMember = memberRandomizer.nextInt(members.size());
        MemberImpl newMember = members.get(randomMember);
        MemberMapPair[] memberMapPairs = new MemberMapPair[1];
        memberMapPairs[0] = new MemberMapPair(newMember, name);

        OperationService operationService = nodeEngine.getOperationService();
        operationService
                .send(new ReplicatedMapPostJoinOperation(memberMapPairs, ReplicatedMapPostJoinOperation.DEFAULT_CHUNK_SIZE),
                        newMember.getAddress());
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    @Override
    public void replicate(MultiReplicationMessage message) {
        distributeReplicationMessage(message, true);
    }

    @Override
    public void replicate(ReplicationMessage message) {
        distributeReplicationMessage(message, true);
    }

    public void setPreReplicationHook(PreReplicationHook preReplicationHook) {
        this.preReplicationHook = preReplicationHook;
    }

    public int getLocalMemberHash() {
        return localMemberHash;
    }

    protected void incrementClock(VectorClock vectorClock) {
        final AtomicInteger clock = vectorClock.clocks.get(localMember);
        if (clock != null) {
            clock.incrementAndGet();
        } else {
            vectorClock.clocks.put(localMember, new AtomicInteger(1));
        }
    }

    protected Object getMutex(final Object key) {
        return mutexes[key.hashCode() != Integer.MIN_VALUE ? Math.abs(key.hashCode()) % mutexes.length : 0];
    }

    private void initializeListeners() {
        List<ListenerConfig> listenerConfigs = replicatedMapConfig.getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                addEntryListener(listener, null);
            }
        }
    }

    private void checkState() {
        if (!loaded.get()) {
            if (!replicatedMapConfig.isAsyncFillup()) {
                while (true) {
                    waitForLoadedLock.lock();
                    try {
                        if (!loaded.get()) {
                            waitForLoadedCondition.await();
                        }
                        // If it is a spurious wakeup we restart waiting
                        if (!loaded.get()) {
                            continue;
                        }
                        // Otherwise return here
                        return;
                    } catch (InterruptedException e) {
                        throw new IllegalStateException("Synchronous loading of ReplicatedMap '" + name + "' failed.", e);
                    } finally {
                        waitForLoadedLock.unlock();
                    }
                }
            }
        }
    }

    private ScheduledEntry<K, V> cancelTtlEntry(K key) {
        return ttlEvictionScheduler.cancel(key);
    }

    private ScheduledEntry<K, V> getTtlEntry(K key) {
        return ttlEvictionScheduler.get(key);
    }

    private boolean scheduleTtlEntry(long delayMillis, K key, V object) {
        return ttlEvictionScheduler.schedule(delayMillis, key, object);
    }

    private void processUpdateMessage(ReplicationMessage update) {
        if (localMember.equals(update.getOrigin())) {
            return;
        }
        mapStats.incrementReceivedReplicationEvents();
        K marshalledKey = (K) marshallKey(update.getKey());
        synchronized (getMutex(marshalledKey)) {
            final ReplicatedRecord<K, V> localEntry = storage.get(marshalledKey);
            if (localEntry == null) {
                if (!update.isRemove()) {
                    V marshalledValue = (V) marshallValue(update.getValue());
                    VectorClock vectorClock = update.getVectorClock();
                    int updateHash = update.getUpdateHash();
                    long ttlMillis = update.getTtlMillis();
                    storage.put(marshalledKey,
                            new ReplicatedRecord<K, V>(marshalledKey, marshalledValue, vectorClock, updateHash, ttlMillis));
                    if (ttlMillis > 0) {
                        scheduleTtlEntry(ttlMillis, marshalledKey, null);
                    } else {
                        cancelTtlEntry(marshalledKey);
                    }
                    fireEntryListenerEvent(update.getKey(), null, update.getValue());
                }
            } else {
                final VectorClock currentVectorClock = localEntry.getVectorClock();
                final VectorClock updateVectorClock = update.getVectorClock();
                if (VectorClock.happenedBefore(updateVectorClock, currentVectorClock)) {
                    // ignore the update. This is an old update
                } else if (VectorClock.happenedBefore(currentVectorClock, updateVectorClock)) {
                    // A new update happened
                    applyTheUpdate(update, localEntry);
                } else {
                    if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                        applyTheUpdate(update, localEntry);
                    } else {
                        applyVector(updateVectorClock, currentVectorClock);
                        incrementClock(currentVectorClock);
                        distributeReplicationMessage(
                                new ReplicationMessage(name, update.getKey(), localEntry.getValue(), currentVectorClock,
                                        localMember, localEntry.getLatestUpdateHash(), update.getTtlMillis()), true
                        );
                    }
                }
            }
        }
    }

    private void applyTheUpdate(ReplicationMessage<K, V> update, ReplicatedRecord<K, V> localEntry) {
        VectorClock localVectorClock = localEntry.getVectorClock();
        VectorClock remoteVectorClock = update.getVectorClock();
        K marshalledKey = (K) marshallKey(update.getKey());
        V marshalledValue = (V) marshallValue(update.getValue());
        long ttlMillis = update.getTtlMillis();
        Object oldValue = localEntry.setValue(marshalledValue, update.getUpdateHash(), ttlMillis);

        applyVector(remoteVectorClock, localVectorClock);
        if (ttlMillis > 0) {
            scheduleTtlEntry(ttlMillis, marshalledKey, null);
        } else {
            cancelTtlEntry(marshalledKey);
        }

        V unmarshalledOldValue = (V) unmarshallValue(oldValue);
        if (unmarshalledOldValue == null || !unmarshalledOldValue.equals(update.getValue()) || update.getTtlMillis() != localEntry
                .getTtlMillis()) {
            fireEntryListenerEvent(update.getKey(), unmarshalledOldValue, update.getValue());
        }
    }

    private void applyVector(VectorClock update, VectorClock current) {
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

    private ScheduledExecutorService getExecutorService(NodeEngine nodeEngine, ReplicatedMapConfig replicatedMapConfig) {
        ScheduledExecutorService es = replicatedMapConfig.getReplicatorExecutorService();
        if (es == null) {
            es = nodeEngine.getExecutionService().getDefaultScheduledExecutor();
        }
        return new WrappedExecutorService(es);
    }

    private void fireEntryListenerEvent(Object key, Object oldValue, Object value) {
        EntryEventType eventType =
                value == null ? EntryEventType.REMOVED : oldValue == null ? EntryEventType.ADDED : EntryEventType.UPDATED;
        EntryEvent event = new EntryEvent(name, localMember, eventType.getType(), key, oldValue, value);

        Collection<EventRegistration> registrations = eventService.getRegistrations(ReplicatedMapService.SERVICE_NAME, name);
        if (registrations.size() > 0) {
            eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, event, name.hashCode());
        }
    }

    private Collection<EventRegistration> filterEventRegistrations(Collection<EventRegistration> eventRegistrations) {
        Address address = ((MemberImpl) localMember).getAddress();
        List<EventRegistration> registrations = new ArrayList<EventRegistration>(eventRegistrations);
        Iterator<EventRegistration> iterator = registrations.iterator();
        while (iterator.hasNext()) {
            EventRegistration registration = iterator.next();
            if (address.equals(registration.getSubscriber())) {
                iterator.remove();
            }
        }
        return registrations;
    }

    private PreReplicationHook getPreReplicationHook() {
        if (!allowReplicationHooks) {
            return null;
        }
        return preReplicationHook;
    }

    private void distributeReplicationMessage(final Object message, final boolean forceSend) {
        final PreReplicationHook preReplicationHook = getPreReplicationHook();
        if (forceSend || preReplicationHook == null) {
            Collection<EventRegistration> registrations = filterEventRegistrations(
                    eventService.getRegistrations(ReplicatedMapService.SERVICE_NAME, ReplicatedMapService.EVENT_TOPIC_NAME));
            eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, message, name.hashCode());
        } else {
            executionService.execute("hz:replicated-map", new Runnable() {
                @Override
                public void run() {
                    if (message instanceof MultiReplicationMessage) {
                        preReplicationHook
                                .preReplicateMultiMessage((MultiReplicationMessage) message, AbstractReplicatedRecordStore.this);
                    } else {
                        preReplicationHook.preReplicateMessage((ReplicationMessage) message, AbstractReplicatedRecordStore.this);
                    }
                }
            });
        }
    }

    private void processMessageCache() {
        ReplicationMessage[] replicationMessages = null;
        replicationMessageCacheLock.lock();
        try {
            final int size = replicationMessageCache.size();
            if (size > 0) {
                replicationMessages = replicationMessageCache.toArray(new ReplicationMessage[size]);
                replicationMessageCache.clear();
            }
        } finally {
            replicationMessageCacheLock.unlock();
        }
        if (replicationMessages != null) {
            MultiReplicationMessage message = new MultiReplicationMessage(name, replicationMessages);
            distributeReplicationMessage(message, false);
        }
    }

    private class RemoteFillupTask
            implements Runnable {

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
            List<ReplicatedRecord<K, V>> replicatedRecords = new ArrayList<ReplicatedRecord<K, V>>(storage.values());
            for (int i = 0; i < replicatedRecords.size(); i++) {
                ReplicatedRecord<K, V> replicatedRecord = replicatedRecords.get(i);
                processReplicatedRecord(replicatedRecord, i == replicatedRecords.size() - 1);
            }
        }

        private void processReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord, boolean finalRecord) {
            Object marshalledKey = marshallKey(replicatedRecord.getKey());
            synchronized (getMutex(marshalledKey)) {
                pushReplicatedRecord(replicatedRecord, finalRecord);
            }
        }

        private void pushReplicatedRecord(ReplicatedRecord<K, V> replicatedRecord, boolean finalRecord) {
            if (recordCachePos == chunkSize) {
                sendChunk(finalRecord);
            }

            int hash = replicatedRecord.getLatestUpdateHash();
            Object key = unmarshallKey(replicatedRecord.getKey());
            Object value = unmarshallValue(replicatedRecord.getValue());
            VectorClock vectorClock = VectorClock.copyVector(replicatedRecord.getVectorClock());
            long ttlMillis = replicatedRecord.getTtlMillis();
            recordCache[recordCachePos++] = new ReplicatedRecord(key, value, vectorClock, hash, ttlMillis);

            if (finalRecord) {
                sendChunk(finalRecord);
            }
        }

        private void sendChunk(boolean finalChunk) {
            if (recordCachePos > 0) {
                Operation operation = new ReplicatedMapInitChunkOperation(name, localMember, recordCache, recordCachePos,
                        finalChunk);
                operationService.send(operation, callerAddress);

                // Reset chunk cache and pos
                recordCache = new ReplicatedRecord[chunkSize];
                recordCachePos = 0;
            }
        }
    }

    private class ReplicationCachedSenderTask
            implements Runnable {

        @Override
        public void run() {
            processMessageCache();
        }
    }

}
