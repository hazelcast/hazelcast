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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.replicatedmap.PreReplicationHook;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.ReplicationChannel;
import com.hazelcast.replicatedmap.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.operation.ReplicatedMapPostJoinOperation;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements the actual replication logic for replicated map
 *
 * @param <K>
 * @param <V>
 */
public class ReplicationPublisher<K, V>
        implements ReplicationChannel {

    private static final String SERVICE_NAME = ReplicatedMapService.SERVICE_NAME;
    private static final String EVENT_TOPIC_NAME = ReplicatedMapService.EVENT_TOPIC_NAME;
    private static final int MAX_MESSAGE_CACHE_SIZE = 1000;

    private final List<ReplicationMessage> replicationMessageCache = new ArrayList<ReplicationMessage>();
    private final Lock replicationMessageCacheLock = new ReentrantLock();
    private final Random memberRandomizer = new Random();

    private final ScheduledExecutorService executorService;
    private final ExecutionService executionService;
    private final EventService eventService;
    private final NodeEngine nodeEngine;

    private final AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore;
    private final InternalReplicatedMapStorage<K, V> storage;
    private final ReplicatedMapConfig replicatedMapConfig;
    private final LocalReplicatedMapStatsImpl mapStats;
    private final Member localMember;
    private final String name;

    private final boolean allowReplicationHooks;
    private volatile PreReplicationHook preReplicationHook;

    ReplicationPublisher(AbstractBaseReplicatedRecordStore<K, V> replicatedRecordStore, NodeEngine nodeEngine) {
        this.replicatedRecordStore = replicatedRecordStore;
        this.nodeEngine = nodeEngine;

        this.name = replicatedRecordStore.getName();
        this.storage = replicatedRecordStore.storage;
        this.mapStats = replicatedRecordStore.mapStats;
        this.eventService = nodeEngine.getEventService();
        this.localMember = replicatedRecordStore.localMember;
        this.executionService = nodeEngine.getExecutionService();
        this.replicatedMapConfig = replicatedRecordStore.replicatedMapConfig;
        this.executorService = getExecutorService(nodeEngine, replicatedMapConfig);

        this.allowReplicationHooks = Boolean.parseBoolean(System.getProperty("hazelcast.repmap.hooks.allowed", "false"));
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

    public void publishReplicatedMessage(ReplicationMessage message) {
        if (replicatedMapConfig.getReplicationDelayMillis() == 0) {
            distributeReplicationMessage(message, false);
        } else {
            replicationMessageCacheLock.lock();
            try {
                replicationMessageCache.add(message);
                if (replicationMessageCache.size() == 1) {
                    ReplicationCachedSenderTask task = new ReplicationCachedSenderTask(this);
                    long replicationDelayMillis = replicatedMapConfig.getReplicationDelayMillis();
                    executorService.schedule(task, replicationDelayMillis, TimeUnit.MILLISECONDS);
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

    void destroy() {
        executorService.shutdownNow();
    }

    void processMessageCache() {
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

    void distributeReplicationMessage(final Object message, final boolean forceSend) {
        final PreReplicationHook preReplicationHook = getPreReplicationHook();
        if (forceSend || preReplicationHook == null) {
            Collection<EventRegistration> eventRegistrations = eventService.getRegistrations(SERVICE_NAME, EVENT_TOPIC_NAME);
            Collection<EventRegistration> registrations = filterEventRegistrations(eventRegistrations);
            eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, message, name.hashCode());
        } else {
            executionService.execute("hz:replicated-map", new Runnable() {
                @Override
                public void run() {
                    if (message instanceof MultiReplicationMessage) {
                        preReplicationHook.preReplicateMultiMessage((MultiReplicationMessage) message, ReplicationPublisher.this);
                    } else {
                        preReplicationHook.preReplicateMessage((ReplicationMessage) message, ReplicationPublisher.this);
                    }
                }
            });
        }
    }

    public void queuePreProvision(Address callerAddress, int chunkSize) {
        RemoteProvisionTask task = new RemoteProvisionTask(replicatedRecordStore, nodeEngine, callerAddress, chunkSize);
        executionService.execute("hz:replicated-map", task);
    }

    public void retryWithDifferentReplicationNode(Member member) {
        List<MemberImpl> members = new ArrayList<MemberImpl>(nodeEngine.getClusterService().getMemberList());
        members.remove(member);

        // If there are less than two members there is not other possible candidate to replicate from
        if (members.size() < 2) {
            return;
        }
        sendPreProvisionRequest(members);
    }

    void sendPreProvisionRequest(List<MemberImpl> members) {
        if (members.size() == 0) {
            return;
        }
        int randomMember = memberRandomizer.nextInt(members.size());
        MemberImpl newMember = members.get(randomMember);
        ReplicatedMapPostJoinOperation.MemberMapPair[] memberMapPairs = new ReplicatedMapPostJoinOperation.MemberMapPair[1];
        memberMapPairs[0] = new ReplicatedMapPostJoinOperation.MemberMapPair(newMember, name);

        OperationService operationService = nodeEngine.getOperationService();
        int defaultChunkSize = ReplicatedMapPostJoinOperation.DEFAULT_CHUNK_SIZE;
        ReplicatedMapPostJoinOperation op = new ReplicatedMapPostJoinOperation(memberMapPairs, defaultChunkSize);
        operationService.send(op, newMember.getAddress());
    }

    private void processUpdateMessage(ReplicationMessage update) {
        if (localMember.equals(update.getOrigin())) {
            return;
        }
        mapStats.incrementReceivedReplicationEvents();
        if (update.getKey() instanceof String) {
            String key = (String) update.getKey();
            if (AbstractReplicatedRecordStore.CLEAR_REPLICATION_MAGIC_KEY.equals(key)) {
                storage.clear();
                return;
            }
        }

        K marshalledKey = (K) replicatedRecordStore.marshallKey(update.getKey());
        synchronized (replicatedRecordStore.getMutex(marshalledKey)) {
            final ReplicatedRecord<K, V> localEntry = storage.get(marshalledKey);
            if (localEntry == null) {
                if (!update.isRemove()) {
                    V marshalledValue = (V) replicatedRecordStore.marshallValue(update.getValue());
                    VectorClock vectorClock = update.getVectorClock();
                    int updateHash = update.getUpdateHash();
                    long ttlMillis = update.getTtlMillis();
                    storage.put(marshalledKey,
                            new ReplicatedRecord<K, V>(marshalledKey, marshalledValue, vectorClock, updateHash, ttlMillis));
                    if (ttlMillis > 0) {
                        replicatedRecordStore.scheduleTtlEntry(ttlMillis, marshalledKey, null);
                    } else {
                        replicatedRecordStore.cancelTtlEntry(marshalledKey);
                    }
                    replicatedRecordStore.fireEntryListenerEvent(update.getKey(), null, update.getValue());
                }
            } else {
                final VectorClock currentVectorClock = localEntry.getVectorClock();
                final VectorClock updateVectorClock = update.getVectorClock();
                if (VectorClock.happenedBefore(updateVectorClock, currentVectorClock)) {
                    // ignore the update. This is an old update
                    return;

                } else if (VectorClock.happenedBefore(currentVectorClock, updateVectorClock)) {
                    // A new update happened
                    applyTheUpdate(update, localEntry);

                } else {
                    if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                        applyTheUpdate(update, localEntry);
                    } else {
                        currentVectorClock.applyVector(updateVectorClock);
                        currentVectorClock.incrementClock(localMember);

                        Object key = update.getKey();
                        V value = localEntry.getValue();
                        long ttlMillis = update.getTtlMillis();
                        int latestUpdateHash = localEntry.getLatestUpdateHash();
                        ReplicationMessage message = new ReplicationMessage(name, key, value, currentVectorClock, localMember,
                                latestUpdateHash, ttlMillis);

                        distributeReplicationMessage(message, true);
                    }
                }
            }
        }
    }

    private void applyTheUpdate(ReplicationMessage<K, V> update, ReplicatedRecord<K, V> localEntry) {
        VectorClock localVectorClock = localEntry.getVectorClock();
        VectorClock remoteVectorClock = update.getVectorClock();
        K marshalledKey = (K) replicatedRecordStore.marshallKey(update.getKey());
        V marshalledValue = (V) replicatedRecordStore.marshallValue(update.getValue());
        long ttlMillis = update.getTtlMillis();
        Object oldValue = localEntry.setValue(marshalledValue, update.getUpdateHash(), ttlMillis);

        localVectorClock.applyVector(remoteVectorClock);
        if (ttlMillis > 0) {
            replicatedRecordStore.scheduleTtlEntry(ttlMillis, marshalledKey, null);
        } else {
            replicatedRecordStore.cancelTtlEntry(marshalledKey);
        }

        V unmarshalledOldValue = (V) replicatedRecordStore.unmarshallValue(oldValue);
        if (unmarshalledOldValue == null || !unmarshalledOldValue.equals(update.getValue()) || update.getTtlMillis() != localEntry
                .getTtlMillis()) {
            replicatedRecordStore.fireEntryListenerEvent(update.getKey(), unmarshalledOldValue, update.getValue());
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

    private ScheduledExecutorService getExecutorService(NodeEngine nodeEngine, ReplicatedMapConfig replicatedMapConfig) {
        ScheduledExecutorService es = replicatedMapConfig.getReplicatorExecutorService();
        if (es == null) {
            es = nodeEngine.getExecutionService().getDefaultScheduledExecutor();
        }
        return new WrappedExecutorService(es);
    }
}
