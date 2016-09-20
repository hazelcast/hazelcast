/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ExecutionService;
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

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.util.Collections.singletonList;

/**
 * Sends invalidations to Near Cache in batches.
 */
public class BatchInvalidator extends AbstractNearCacheInvalidator {

    private static final String INVALIDATION_EXECUTOR_NAME = BatchInvalidator.class.getName();

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

    private final int batchSize;
    private final String nodeShutdownListenerId;

    public BatchInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        super(mapServiceContext, nearCacheProvider);
        batchSize = getBatchSize();
        nodeShutdownListenerId = registerNodeShutdownListener();
        startBackgroundBatchProcessor();
    }

    @Override
    public void invalidate(Data key, String mapName, String sourceUuid) {
        accumulateOrInvalidate(key, mapName, sourceUuid);
    }

    private void accumulateOrInvalidate(Data key, String mapName, String sourceUuid) {
        assert mapName != null;
        assert sourceUuid != null;

        InvalidationQueue invalidationQueue = getOrPutIfAbsent(invalidationQueues, mapName, invalidationQueueConstructor);
        invalidationQueue.offer(new SingleNearCacheInvalidation(toHeapData(key), mapName, sourceUuid));

        if (invalidationQueue.size() >= batchSize) {
            invalidate(invalidationQueue, true, mapName);
        }
    }

    private void invalidate(InvalidationQueue invalidationQueue, boolean offloadEventSending, String mapName) {
        if (invalidationQueue == null) {
            return;
        }
        // if still in progress, no need to another attempt, so just return
        if (!invalidationQueue.tryAcquire()) {
            return;
        }

        final int size = Math.min(batchSize, invalidationQueue.size());
        if (size == 0) {
            return;
        }

        List<SingleNearCacheInvalidation> invalidations = createInvalidations(invalidationQueue, size);
        try {
            sendInvalidations(invalidations, offloadEventSending, mapName);
        } finally {
            invalidationQueue.release();
        }
    }

    private static List<SingleNearCacheInvalidation> createInvalidations(InvalidationQueue invalidationQueue, int size) {
        boolean foundClearEvent = false;
        List<SingleNearCacheInvalidation> invalidations = new ArrayList<SingleNearCacheInvalidation>(size);

        for (int i = 0; i < size; i++) {
            SingleNearCacheInvalidation invalidation = invalidationQueue.poll();
            if (invalidation == null) {
                break;
            }

            if (foundClearEvent) {
                continue;
            }

            if (invalidation.getKey() == null) {
                foundClearEvent = true;
                invalidations = singletonList(invalidation);
            } else {
                invalidations.add(invalidation);
            }

        }
        return invalidations;
    }

    private void sendInvalidations(List<SingleNearCacheInvalidation> invalidations,
                                   boolean offloadEventSending, String mapName) {
        if (offloadEventSending) {
            nodeEngine.getExecutionService().execute(mapName, new EventSender(invalidations, mapName));
        } else {
            sendInvalidationsInternal(invalidations, mapName);
        }
    }

    private final class EventSender implements Runnable {

        private final List<SingleNearCacheInvalidation> invalidationList;
        private final String mapName;

        public EventSender(List<SingleNearCacheInvalidation> invalidationList, String mapName) {
            this.invalidationList = invalidationList;
            this.mapName = mapName;
        }

        @Override
        public void run() {
            sendInvalidationsInternal(invalidationList, mapName);
        }
    }

    private void sendInvalidationsInternal(final List<SingleNearCacheInvalidation> invalidationList, final String mapName) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            List<SingleNearCacheInvalidation> filteredInvalidations = null;
            for (SingleNearCacheInvalidation invalidation : invalidationList) {
                if (canSendInvalidationEvent(registration, invalidation.getSourceUuid())) {

                    if (filteredInvalidations == null) {
                        filteredInvalidations = new ArrayList<SingleNearCacheInvalidation>();
                    }

                    filteredInvalidations.add(invalidation);
                }
            }

            if (filteredInvalidations != null) {
                BatchNearCacheInvalidation invalidation = new BatchNearCacheInvalidation(filteredInvalidations, mapName);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, mapName.hashCode());
            }
        }
    }

    private void sendSingleInvalidationEvent(SingleNearCacheInvalidation invalidation) {
        final String mapName = invalidation.getName();

        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            if (canSendInvalidationEvent(registration, invalidation.getSourceUuid())) {
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, orderKey(invalidation));
            }
        }
    }

    /**
     * Sends remaining invalidation events in this invalidator's queues to the recipients.
     */
    private String registerNodeShutdownListener() {
        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        return lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    Set<Map.Entry<String, InvalidationQueue>> entries = invalidationQueues.entrySet();
                    for (Map.Entry<String, InvalidationQueue> entry : entries) {
                        invalidate(entry.getValue(), false, entry.getKey());
                    }
                }
            }
        });
    }

    private int getBatchSize() {
        return nodeEngine.getProperties().getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
    }

    private int getBackgroundProcessorRunPeriodSeconds() {
        return nodeEngine.getProperties().getInteger(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
    }

    private void startBackgroundBatchProcessor() {
        int periodSeconds = getBackgroundProcessorRunPeriodSeconds();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(INVALIDATION_EXECUTOR_NAME,
                new MapBatchInvalidationEventSender(), periodSeconds, periodSeconds, TimeUnit.SECONDS);

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
                    invalidate(invalidationQueue, false, mapName);
                }
            }
        }
    }

    private static class InvalidationQueue extends ConcurrentLinkedQueue<SingleNearCacheInvalidation> {

        private final AtomicInteger elementCount = new AtomicInteger(0);
        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        @Override
        public int size() {
            return elementCount.get();
        }

        @Override
        public boolean offer(SingleNearCacheInvalidation invalidation) {
            boolean offered = super.offer(invalidation);
            if (offered) {
                elementCount.incrementAndGet();
            }
            return offered;
        }

        @Override
        public SingleNearCacheInvalidation poll() {
            SingleNearCacheInvalidation invalidation = super.poll();
            if (invalidation != null) {
                elementCount.decrementAndGet();
            }
            return invalidation;
        }

        public boolean tryAcquire() {
            return flushingInProgress.compareAndSet(false, true);
        }

        public void release() {
            flushingInProgress.set(false);
        }

        @Override
        public boolean add(SingleNearCacheInvalidation invalidation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SingleNearCacheInvalidation remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends SingleNearCacheInvalidation> c) {
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

    @Override
    public void destroy(String mapName, String sourceUuid) {
        InvalidationQueue invalidationQueue = invalidationQueues.remove(mapName);
        if (invalidationQueue != null) {
            sendSingleInvalidationEvent(new SingleNearCacheInvalidation(null, mapName, sourceUuid));
        }
    }

    @Override
    public void shutdown() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.shutdownExecutor(INVALIDATION_EXECUTOR_NAME);

        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        lifecycleService.removeLifecycleListener(nodeShutdownListenerId);

        invalidationQueues.clear();
    }

    @Override
    public void reset() {
        invalidationQueues.clear();
    }
}
