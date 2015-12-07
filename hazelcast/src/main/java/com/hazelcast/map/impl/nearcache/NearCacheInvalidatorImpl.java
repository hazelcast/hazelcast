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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.InvalidateNearCacheOperation;
import com.hazelcast.map.impl.operation.NearCacheKeySetInvalidationOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Default implementation of {@link NearCacheInvalidator}
 *
 * @see NearCacheInvalidator
 */
public class NearCacheInvalidatorImpl implements NearCacheInvalidator {

    /**
     * Creates an invalidation-queue for a map.
     */
    private final ConstructorFunction<String, InvalidationQueue> invalidationQueueConstructor
            = new ConstructorFunction<String, InvalidationQueue>() {
        @Override
        public InvalidationQueue createNew(String mapName) {
            return new InvalidationQueue();
        }
    };

    /**
     * map-name to invalidation-queue mappings.
     */
    private final ConcurrentMap<String, InvalidationQueue> invalidationQueues
            = new ConcurrentHashMap<String, InvalidationQueue>();

    private final EventService eventService;
    private final NodeEngine nodeEngine;
    private final MapServiceContext mapServiceContext;
    private final NearCacheProvider nearCacheProvider;
    private final boolean batchingEnabled;
    private final int batchSize;

    NearCacheInvalidatorImpl(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        this.mapServiceContext = mapServiceContext;
        this.nearCacheProvider = nearCacheProvider;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.eventService = nodeEngine.getEventService();
        this.batchSize = getBatchSize();
        this.batchingEnabled = isBatchingEnabled(batchSize);
        if (batchingEnabled) {
            startBackgroundBatchProcessor();
            handleBatchesOnNodeShutdown();
        }
    }

    private void handleBatchesOnNodeShutdown() {
        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    Set<String> mapNames = NearCacheInvalidatorImpl.this.nearCacheProvider.nearCacheMap.keySet();
                    for (String mapName : mapNames) {
                        InvalidationQueue invalidationQueue = invalidationQueues.get(mapName);
                        sendBatchInvalidation(mapName, invalidationQueue);
                    }
                }
            }
        });
    }

    private void startBackgroundBatchProcessor() {
        int periodSeconds = getBackgroundProcessorRunPeriodSeconds();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleAtFixedRate(SERVICE_NAME + ":batchBackgroundProcessor",
                new MapBatchInvalidationEventSender(), periodSeconds, periodSeconds, TimeUnit.SECONDS);
    }

    private int getBatchSize() {
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        return groupProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
    }

    private boolean isBatchingEnabled(int batchSize) {
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        return groupProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;
    }

    private int getBackgroundProcessorRunPeriodSeconds() {
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        return groupProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
    }

    @Override
    public void invalidateLocalNearCache(String mapName, Data key) {
        if (!isServerNearCacheInvalidationEnabled(mapName)) {
            return;
        }

        NearCache nearCache = nearCacheProvider.getOrNullNearCache(mapName);
        if (nearCache != null) {
            nearCache.remove(key);
        }
    }

    @Override
    public void invalidateLocalNearCache(String mapName, Collection<Data> keys) {
        if (!isServerNearCacheInvalidationEnabled(mapName)) {
            return;
        }
        NearCache nearCache = nearCacheProvider.getOrNullNearCache(mapName);
        if (nearCache != null) {
            for (Data key : keys) {
                nearCache.remove(key);
            }
        }
    }

    @Override
    public void clearLocalNearCache(String mapName, String sourceUuid) {
        if (!isServerNearCacheInvalidationEnabled(mapName)) {
            return;
        }

        NearCache nearCache = nearCacheProvider.getOrNullNearCache(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    @Override
    public void clearNearCaches(String mapName, boolean owner, String sourceUuid) {
        if (owner) {
            sendRemoteCleaningInvalidation(mapName, sourceUuid);
        }

        clearLocalNearCache(mapName, sourceUuid);

    }

    @Override
    public void invalidateNearCaches(String mapName, Data key, String sourceUuid) {
        // remote near-cache invalidation
        sendRemoteInvalidation(mapName, key, sourceUuid);
        // local near-cache invalidation: this invalidation is for the case the data is cached before partition is owned/migrated
        invalidateLocalNearCache(mapName, key);
    }

    @Override
    public void invalidateNearCaches(String mapName, List<Data> keys, String sourceUuid) {
        if (isEmpty(keys)) {
            return;
        }
        // remote near-cache invalidation
        sendRemoteInvalidation(mapName, keys, sourceUuid);
        // local near-cache invalidation: this invalidation is for the case the data is cached before partition is owned/migrated
        invalidateLocalNearCache(mapName, keys);
    }

    @Override
    public void flushAndRemoveInvalidationQueue(String mapName) {
        InvalidationQueue invalidationQueue = invalidationQueues.remove(mapName);
        if (invalidationQueue != null) {
            sendRemoteCleaningInvalidation(mapName, null);
        }
    }

    public void accumulateOrSendBatchInvalidation(String mapName, Data key) {
        if (!isServerNearCacheInvalidationEnabled(mapName)
                && !hasInvalidationListener(mapName)) {
            return;
        }

        InvalidationQueue invalidationQueue = getOrPutIfAbsent(invalidationQueues, mapName, invalidationQueueConstructor);
        invalidationQueue.offer(mapServiceContext.toData(key));
        if (invalidationQueue.size() >= batchSize) {
            sendBatchInvalidation(mapName, invalidationQueue);
        }
    }

    private boolean isServerNearCacheInvalidationEnabled(String mapName) {
        MapContainer mapContainer = mapServiceContext.getOrNullMapContainer(mapName);
        if (mapContainer == null) {
            return false;
        }
        return mapContainer.isServerNearCacheInvalidationEnabled();
    }

    protected boolean hasInvalidationListener(String mapName) {
        MapContainer mapContainer = mapServiceContext.getOrNullMapContainer(mapName);
        if (mapContainer == null) {
            return false;
        }

        return mapContainer.hasInvalidationListener();
    }

    private void sendBatchInvalidation(String mapName, InvalidationQueue invalidationQueue) {
        if (invalidationQueue == null) {
            return;
        }
        // If still in progress, no need to another attempt. So just return.
        if (!invalidationQueue.tryAcquire()) {
            return;
        }

        int size = invalidationQueue.size();
        List<Data> keysToBeInvalidated = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = invalidationQueue.poll();
            if (key == null) {
                break;
            }
            keysToBeInvalidated.add(key);
        }

        try {
            sendInvalidationToServerNearCaches(mapName, keysToBeInvalidated);
            sendInvalidationToClientNearCaches(mapName, keysToBeInvalidated, null);
        } finally {
            invalidationQueue.release();
        }
    }

    private void sendRemoteInvalidation(String mapName, Data key, String sourceUuid) {
        if (batchingEnabled) {
            accumulateOrSendBatchInvalidation(mapName, key);
        } else {
            sendInvalidationToServerNearCaches(mapName, key);
            sendInvalidationToClientNearCaches(mapName, key, sourceUuid);
        }
    }

    private void sendRemoteInvalidation(String mapName, List<Data> keys, String sourceUuid) {
        if (batchingEnabled) {
            for (Data key : keys) {
                accumulateOrSendBatchInvalidation(mapName, key);
            }
        } else {
            sendInvalidationToServerNearCaches(mapName, keys);
            sendInvalidationToClientNearCaches(mapName, keys, sourceUuid);
        }
    }

    private void sendRemoteCleaningInvalidation(String mapName, String sourceUuid) {
        // only send invalidation event to clients, server near-caches are cleared by ClearOperation.
        sendInvalidationToClientNearCaches(mapName, null, sourceUuid);
    }

    private void sendInvalidationToClientNearCaches(String mapName, Object invalidationData, String sourceUuid) {
        if (!hasInvalidationListener(mapName)) {
            return;
        }

        Invalidation invalidation = null;
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();
            if (filter instanceof EventListenerFilter && filter.eval(INVALIDATION.getType())) {
                if (invalidation == null) {
                    invalidation = newInvalidation(mapName, invalidationData, sourceUuid);
                }

                Object orderKey = getOrderKey(mapName, invalidation);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, orderKey.hashCode());
            }
        }
    }

    private Invalidation newInvalidation(String mapName, Object invalidationData, String sourceUuid) {
        if (invalidationData instanceof Data) {
            return new SingleNearCacheInvalidation(mapName, ((Data) invalidationData), sourceUuid);
        }

        if (invalidationData instanceof List) {
            return new BatchNearCacheInvalidation(mapName, ((List<Data>) invalidationData), sourceUuid);
        }

        if (invalidationData == null) {
            return new CleaningNearCacheInvalidation(mapName, sourceUuid);
        }

        throw new IllegalArgumentException("Unexpected near cache invalidation data type found = ["
                + invalidationData + ']');
    }

    public static Object getOrderKey(String mapName, Invalidation invalidation) {
        if (invalidation instanceof SingleNearCacheInvalidation) {
            return ((SingleNearCacheInvalidation) invalidation).getKey();
        } else {
            return mapName;
        }
    }

    private void sendInvalidationToServerNearCaches(String mapName, Data key) {
        if (!isServerNearCacheInvalidationEnabled(mapName)) {
            return;
        }
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                Operation operation = new InvalidateNearCacheOperation(mapName, key).setServiceName(SERVICE_NAME);
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
    }

    private void sendInvalidationToServerNearCaches(String mapName, List<Data> keys) {
        if (!isServerNearCacheInvalidationEnabled(mapName)) {
            return;
        }

        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember()) {
                    continue;
                }
                Operation operation = new NearCacheKeySetInvalidationOperation(mapName, keys).setServiceName(SERVICE_NAME);
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                nodeEngine.getLogger(getClass()).warning(throwable);
            }
        }
    }

    private static class InvalidationQueue extends ConcurrentLinkedQueue<Data> {

        private final AtomicInteger elementCount = new AtomicInteger(0);
        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        @Override
        public int size() {
            return elementCount.get();
        }

        @Override
        public boolean offer(Data key) {
            boolean offered = super.offer(key);
            if (offered) {
                elementCount.incrementAndGet();
            }
            return offered;
        }

        @Override
        public Data poll() {
            Data polledItem = super.poll();
            if (polledItem != null) {
                elementCount.decrementAndGet();
            }
            return polledItem;
        }

        public boolean tryAcquire() {
            return flushingInProgress.compareAndSet(false, true);
        }

        public void release() {
            flushingInProgress.set(false);
        }

        @Override
        public boolean add(Data key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Data remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends Data> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A background runner which runs periodically and consumes invalidation queues.
     */
    private class MapBatchInvalidationEventSender implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<String, InvalidationQueue> entry : invalidationQueues.entrySet()) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                String mapName = entry.getKey();
                InvalidationQueue invalidationQueue = entry.getValue();
                if (invalidationQueue.size() > 0) {
                    sendBatchInvalidation(mapName, invalidationQueue);
                }
            }
        }

    }


}
