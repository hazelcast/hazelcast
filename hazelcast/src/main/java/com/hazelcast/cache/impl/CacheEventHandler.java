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
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Map;
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
 * Sends cache invalidation events in batch or single as configured.
 */
class CacheEventHandler {

    private final NodeEngine nodeEngine;

    private boolean invalidationMessageBatchEnabled;
    private int invalidationMessageBatchSize;
    private final ConcurrentMap<String, InvalidationEventQueue> invalidationMessageMap =
            new ConcurrentHashMap<String, InvalidationEventQueue>();
    private ScheduledFuture cacheBatchInvalidationMessageSenderScheduler;

    CacheEventHandler(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        invalidationMessageBatchEnabled =
                groupProperties.getBoolean(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED);
        if (invalidationMessageBatchEnabled) {
            invalidationMessageBatchSize =
                    groupProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE);
            int invalidationMessageBatchFreq =
                    groupProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
            cacheBatchInvalidationMessageSenderScheduler = nodeEngine.getExecutionService()
                    .scheduleAtFixedRate(ICacheService.SERVICE_NAME + ":cacheBatchInvalidationMessageSender",
                                         new CacheBatchInvalidationMessageSender(),
                                         invalidationMessageBatchFreq,
                                         invalidationMessageBatchFreq,
                                          TimeUnit.SECONDS);
        }
        nodeEngine.getHazelcastInstance().getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    invalidateAllCaches();
                }
            }
        });
    }

    void publishEvent(CacheEventContext cacheEventContext) {
        final EventService eventService = nodeEngine.getEventService();
        final String cacheName = cacheEventContext.getCacheName();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(ICacheService.SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        final Object eventData;
        final CacheEventType eventType = cacheEventContext.getEventType();
        switch (eventType) {
            case CREATED:
            case UPDATED:
            case REMOVED:
            case EXPIRED:
                final CacheEventData cacheEventData =
                        new CacheEventDataImpl(cacheName, eventType, cacheEventContext.getDataKey(),
                                               cacheEventContext.getDataValue(), cacheEventContext.getDataOldValue(),
                                               cacheEventContext.isOldValueAvailable());
                CacheEventSet eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(cacheEventData);
                eventData = eventSet;
                break;
            case EVICTED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.EVICTED,
                                                   cacheEventContext.getDataKey(), null, null, false);
                break;
            case INVALIDATED:
                eventData = new CacheEventDataImpl(cacheName, CacheEventType.INVALIDATED,
                                                   cacheEventContext.getDataKey(), null, null, false);
                break;
            case COMPLETED:
                CacheEventData completedEventData =
                        new CacheEventDataImpl(cacheName, CacheEventType.COMPLETED, cacheEventContext.getDataKey(),
                                               cacheEventContext.getDataValue(), null, false);
                eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(completedEventData);
                eventData = eventSet;
                break;
            default:
                throw new IllegalArgumentException(
                        "Event Type not defined to create an eventData during publish : " + eventType.name());
        }
        nodeEngine.getEventService().publishEvent(ICacheService.SERVICE_NAME, candidates,
                                                  eventData, cacheEventContext.getOrderKey());
    }

    void publishEvent(String cacheName, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(ICacheService.SERVICE_NAME, cacheName);
        if (candidates.isEmpty()) {
            return;
        }
        nodeEngine.getEventService().publishEvent(ICacheService.SERVICE_NAME, candidates, eventSet, orderKey);
    }

    void sendInvalidationEvent(String name, Data key, String sourceUuid) {
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

    void shutdown() {
        if (cacheBatchInvalidationMessageSenderScheduler != null) {
            cacheBatchInvalidationMessageSenderScheduler.cancel(true);
        }
    }

    private void sendSingleInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(ICacheService.SERVICE_NAME, name);
        if (!registrations.isEmpty()) {
            eventService.publishEvent(ICacheService.SERVICE_NAME, registrations,
                    new CacheSingleInvalidationMessage(name, key, sourceUuid), name.hashCode());

        }
    }

    private void sendBatchInvalidationEvent(String name, Data key, String sourceUuid) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(ICacheService.SERVICE_NAME, name);
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

    private void flushInvalidationMessages(String cacheName, InvalidationEventQueue invalidationMessageQueue) {
        // If still in progress, no need to another attempt. So just ignore.
        if (invalidationMessageQueue.tryAcquire()) {
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
                Collection<EventRegistration> registrations =
                        eventService.getRegistrations(ICacheService.SERVICE_NAME, cacheName);
                if (!registrations.isEmpty()) {
                    eventService.publishEvent(ICacheService.SERVICE_NAME, registrations,
                                              batchInvalidationMessage, cacheName.hashCode());
                }
            } finally {
                invalidationMessageQueue.release();
            }
        }
    }

    private void invalidateAllCaches() {
        for (Map.Entry<String, InvalidationEventQueue> entry : invalidationMessageMap.entrySet()) {
            String cacheName = entry.getKey();
            sendInvalidationEvent(cacheName, null, null);
        }
    }

    private class CacheBatchInvalidationMessageSender implements Runnable {

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            for (Map.Entry<String, InvalidationEventQueue> entry : invalidationMessageMap.entrySet()) {
                if (currentThread.isInterrupted()) {
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

    private static class InvalidationEventQueue extends ConcurrentLinkedQueue<CacheSingleInvalidationMessage> {

        private final AtomicInteger elementCount = new AtomicInteger(0);
        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        private boolean tryAcquire() {
            return flushingInProgress.compareAndSet(false, true);
        }

        private void release() {
            flushingInProgress.set(false);
        }

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
            // We don't support this at the moment, because it is not used at the moment
            throw new UnsupportedOperationException();
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
