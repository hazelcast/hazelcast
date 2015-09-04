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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.client.CacheBatchInvalidationMessage;
import com.hazelcast.cache.impl.client.CacheInvalidationListener;
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionReplicationEvent;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.instance.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;

/**
 * Cache Service is the main access point of JCache implementation.
 * <p>
 * This service is responsible for:
 * <ul>
 * <li>Creating and/or accessing the named {@link com.hazelcast.cache.impl.CacheRecordStore}.</li>
 * <li>Creating/Deleting the cache configuration of the named {@link com.hazelcast.cache.ICache}.</li>
 * <li>Registering/Deregistering of cache listeners.</li>
 * <li>Publish/dispatch cache events.</li>
 * <li>Enabling/Disabling statistic and management.</li>
 * <li>Data migration commit/rollback through {@link com.hazelcast.spi.MigrationAwareService}.</li>
 * </ul>
 * </p>
 * <p><b>WARNING:</b>This service is an optionally registered service which is enabled when {@link javax.cache.Caching}
 * class is found on the classpath.</p>
 * <p>
 * If registered, it will provide all the above cache operations for all partitions of the node which it
 * is registered on.
 * </p>
 * <p><b>Distributed Cache Name</b> is used for providing a unique name to a cache object to overcome cache manager
 * scoping which depends on URI and class loader parameters. It's a simple concatenation of CacheNamePrefix and
 * cache name where CacheNamePrefix is calculated by each cache manager
 * using {@link AbstractHazelcastCacheManager#cacheNamePrefix()}.
 * </p>
 */
public class CacheService extends AbstractCacheService {

    protected boolean invalidationMessageBatchEnabled;
    protected int invalidationMessageBatchSize;
    protected final ConcurrentMap<String, InvalidationEventQueue> invalidationMessageMap =
            new ConcurrentHashMap<String, InvalidationEventQueue>();
    protected ScheduledFuture cacheBatchInvalidationMessageSenderScheduler;

    protected ICacheRecordStore createNewRecordStore(String name, int partitionId) {
        return new CacheRecordStore(name, partitionId, nodeEngine, CacheService.this);
    }

    @Override
    protected void postInit(NodeEngine nodeEngine, Properties properties) {
        super.postInit(nodeEngine, properties);
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        if (groupProperties.getBoolean(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED)) {
            invalidationMessageBatchSize = groupProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE);
            int invalidationMessageBatchFreq = groupProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
            cacheBatchInvalidationMessageSenderScheduler = nodeEngine.getExecutionService()
                    .scheduleAtFixedRate(SERVICE_NAME + ":cacheBatchInvalidationMessageSender",
                            new CacheBatchInvalidationMessageSender(),
                            invalidationMessageBatchFreq,
                            invalidationMessageBatchFreq,
                            TimeUnit.SECONDS);
        }
    }

    @Override
    public void reset() {
        for (String objectName : configs.keySet()) {
            destroyCache(objectName, true, null);
        }
        final CachePartitionSegment[] partitionSegments = segments;
        for (CachePartitionSegment partitionSegment : partitionSegments) {
            if (partitionSegment != null) {
                partitionSegment.clear();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        if (!terminate) {
            if (cacheBatchInvalidationMessageSenderScheduler != null) {
                cacheBatchInvalidationMessageSenderScheduler.cancel(true);
            }
            reset();
        }
    }

    //region MigrationAwareService
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = new CacheReplicationOperation(segment, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }
    //endregion

    /**
     * Registers and {@link CacheInvalidationListener} for specified <code>cacheName</code>.
     *
     * @param name      the name of the cache that {@link CacheEventListener} will be registered for
     * @param listener  the {@link CacheEventListener} to be registered for specified <code>cache</code>
     * @return the id which is unique for current registration
     */
    public String addInvalidationListener(String name, CacheEventListener listener) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, name, listener);
        return registration.getId();
    }

    /**
     * Sends an invalidation event for given <code>cacheName</code> with specified <code>key</code>
     * from mentioned source with <code>sourceUuid</code>.
     *
     * @param name       the name of the cache that invalidation event is sent for
     * @param key        the {@link Data} represents the invalidation event
     * @param sourceUuid an id that represents the source for invalidation event
     */
    @Override
    public void sendInvalidationEvent(String name, Data key, String sourceUuid) {
        if (key == null) {
            sendSingleInvalidationEvent(name, null, sourceUuid);
        } else {
            if (invalidationMessageBatchEnabled) {
                sendBatchInvalidationEvent(name, key, sourceUuid);
            } else {
                sendSingleInvalidationEvent(name, key, sourceUuid);
            }
        }
    }

    protected void sendSingleInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (!registrations.isEmpty()) {
            eventService.publishEvent(SERVICE_NAME, registrations,
                                      new CacheSingleInvalidationMessage(name, key, sourceUuid), name.hashCode());

        }
    }

    protected void sendBatchInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        InvalidationEventQueue invalidationMessageQueue =  invalidationMessageMap.get(name);
        if (invalidationMessageQueue == null) {
            InvalidationEventQueue newInvalidationMessageQueue = new InvalidationEventQueue();
            invalidationMessageQueue = invalidationMessageMap.putIfAbsent(name, newInvalidationMessageQueue);
            if (invalidationMessageQueue == null) {
                invalidationMessageQueue = newInvalidationMessageQueue;
            }
        }
        CacheSingleInvalidationMessage invalidationMessage = new CacheSingleInvalidationMessage(name, key, sourceUuid);
        invalidationMessageQueue.offer(invalidationMessage);
        if (invalidationMessageQueue.size() >= invalidationMessageBatchSize) {
            flushInvalidationMessages(name, invalidationMessageQueue);
        }
    }

    protected void flushInvalidationMessages(String cacheName, InvalidationEventQueue invalidationMessageQueue) {
         // If still in progress, no need to another attempt. So just ignore.
         if (invalidationMessageQueue.flushingInProgress.compareAndSet(false, true)) {
             try {
                 CacheBatchInvalidationMessage batchInvalidationMessage =
                         new CacheBatchInvalidationMessage(cacheName, invalidationMessageQueue.size());
                 CacheSingleInvalidationMessage invalidationMessage;
                 final int size = invalidationMessageQueue.size();
                 // At most, poll from the invalidation queue as the current size of the queue before start to polling.
                 // So skip new invalidation queue items offered while the polling in progress in this round.
                 for (int i = 0; i < size; i++) {
                     invalidationMessage = invalidationMessageQueue.poll();
                     if (invalidationMessage == null) {
                         break;
                     }
                     batchInvalidationMessage.addInvalidationMessage(invalidationMessage);
                 }
                 EventService eventService = nodeEngine.getEventService();
                 Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, cacheName);
                 if (!registrations.isEmpty()) {
                     eventService.publishEvent(SERVICE_NAME, registrations,
                                               batchInvalidationMessage, cacheName.hashCode());
                 }
             } finally {
                 invalidationMessageQueue.flushingInProgress.set(false);
             }
         }
    }

    protected class CacheBatchInvalidationMessageSender implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<String, InvalidationEventQueue> entry : invalidationMessageMap.entrySet()) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                String cacheName = entry.getKey();
                InvalidationEventQueue invalidationMessageQueue = entry.getValue();
                if (invalidationMessageQueue.size() > 0) {
                    flushInvalidationMessages(cacheName, invalidationMessageQueue);
                }
            }
        }

    }

    protected static class InvalidationEventQueue extends ConcurrentLinkedQueue<CacheSingleInvalidationMessage> {

        private final AtomicInteger elementCount = new AtomicInteger(0);
        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        @Override
        public int size() {
            return elementCount.get();
        }

        @Override
        public boolean offer(CacheSingleInvalidationMessage invalidationMessage) {
            boolean offered = super.offer(invalidationMessage);
            if (offered) {
                elementCount.incrementAndGet();
            }
            return offered;
        }

        @Override
        public boolean add(CacheSingleInvalidationMessage invalidationMessage) {
            // We don't support this at the moment, because
            //   - It is not used at the moment
            //   - It may or may not use "offer" method internally and this depends on the implementation
            //     so it may change between different version of Java
            throw new UnsupportedOperationException();
        }

        @Override
        public CacheSingleInvalidationMessage poll() {
            CacheSingleInvalidationMessage polledItem = super.poll();
            if (polledItem != null) {
                elementCount.decrementAndGet();
            }
            return polledItem;
        }

        @Override
        public CacheSingleInvalidationMessage remove() {
            // We don't support this at the moment, because
            //   - It is not used at the moment
            //   - It may or may not use "poll" method internally and this depends on the implementation
            //     so it may change between different version of Java
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            boolean removed = super.remove(o);
            if (removed) {
                elementCount.decrementAndGet();
            }
            return removed;
        }

        @Override
        public boolean addAll(Collection<? extends CacheSingleInvalidationMessage> c) {
            // We don't support this at the moment, because it is not used at the moment
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            // We don't support this at the moment, because it is not used at the moment
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            // We don't support this at the moment, because it is not used at the moment
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            // We don't support this at the moment, because
            //   - It is not used at the moment
            //   - It may or may not use "poll" method internally and this depends on the implementation
            //     so it may change between different version of Java
            throw new UnsupportedOperationException();
        }

    }

}
