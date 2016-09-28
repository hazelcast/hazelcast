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
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
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
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;

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
    private final ExecutionService executionService;

    public BatchInvalidator(NodeEngine nodeEngine) {
        super(nodeEngine);

        this.batchSize = getBatchSize();
        this.nodeShutdownListenerId = registerNodeShutdownListener();
        this.executionService = nodeEngine.getExecutionService();
        startBackgroundBatchProcessor();
    }

    @Override
    protected void invalidateInternal(Invalidation invalidation, int orderKey) {
        String mapName = invalidation.getName();
        InvalidationQueue invalidationQueue = getOrPutIfAbsent(invalidationQueues, mapName, invalidationQueueConstructor);
        invalidationQueue.offer(invalidation);

        if (invalidationQueue.size() >= batchSize) {
            createAndSendInvalidations(mapName, invalidationQueue, true);
        }
    }

    private void createAndSendInvalidations(String mapName, InvalidationQueue invalidationQueue, boolean offloadEventSending) {
        assert invalidationQueue != null;

        if (!invalidationQueue.tryAcquire()) {
            // if still in progress, no need to another attempt, so just return
            return;
        }

        try {
            List<Invalidation> invalidations = createInvalidations(invalidationQueue);
            if (!isEmpty(invalidations)) {
                sendInvalidations(mapName, invalidations, offloadEventSending);
            }
        } finally {
            invalidationQueue.release();
        }
    }

    private List<Invalidation> createInvalidations(InvalidationQueue invalidationQueue) {
        final int size = min(batchSize, invalidationQueue.size());
        if (size == 0) {
            return emptyList();
        }

        List<Invalidation> invalidations = new ArrayList<Invalidation>(size);

        for (int i = 0; i < size; i++) {
            Invalidation invalidation = invalidationQueue.poll();
            if (invalidation == null) {
                break;
            }

            invalidations.add(invalidation);

        }
        return invalidations;
    }

    private void sendInvalidations(String mapName, List<Invalidation> invalidations, boolean offloadEventSending) {
        if (offloadEventSending) {
            executionService.execute(mapName, new EventSender(mapName, invalidations));
        } else {
            sendInvalidations(mapName, invalidations);
        }
    }

    private final class EventSender implements Runnable {

        private final String mapName;
        private final List<Invalidation> invalidations;

        public EventSender(String mapName, List<Invalidation> invalidations) {
            this.mapName = mapName;
            this.invalidations = invalidations;
        }

        @Override
        public void run() {
            sendInvalidations(mapName, invalidations);
        }
    }

    private void sendInvalidations(String mapName, List<Invalidation> invalidations) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            List<Invalidation> selection = filterInvalidations(invalidations, registration.getFilter());
            if (selection != null) {
                Invalidation invalidation = new BatchNearCacheInvalidation(selection, mapName);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, mapName.hashCode());
            }
        }
    }

    private List<Invalidation> filterInvalidations(List<Invalidation> invalidations, EventFilter filter) {
        List<Invalidation> selection = null;
        for (Invalidation invalidation : invalidations) {
            if (canSendInvalidation(filter, invalidation.getSourceUuid())) {
                if (selection == null) {
                    selection = new ArrayList<Invalidation>();
                }
                selection.add(invalidation);
            }
        }
        return selection;
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
                        createAndSendInvalidations(entry.getKey(), entry.getValue(), false);
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
                    createAndSendInvalidations(mapName, invalidationQueue, false);
                }
            }
        }
    }

    @Override
    public void destroy(String mapName, String sourceUuid) {
        InvalidationQueue invalidationQueue = invalidationQueues.remove(mapName);
        if (invalidationQueue != null) {
            invalidateInternal(new ClearNearCacheInvalidation(mapName, sourceUuid), mapName.hashCode());
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

    private static class InvalidationQueue extends ConcurrentLinkedQueue<Invalidation> {
        private final AtomicInteger elementCount = new AtomicInteger(0);

        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        @Override
        public int size() {
            return elementCount.get();
        }

        @Override
        public boolean offer(Invalidation invalidation) {
            boolean offered = super.offer(invalidation);
            if (offered) {
                elementCount.incrementAndGet();
            }
            return offered;
        }

        @Override
        public Invalidation poll() {
            Invalidation invalidation = super.poll();
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
        public boolean add(Invalidation invalidation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Invalidation remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends Invalidation> c) {
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
}
