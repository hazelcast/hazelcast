/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Sends invalidations to Near Cache in batches.
 */
public class BatchInvalidator extends Invalidator {

    private final String invalidationExecutorName;

    /**
     * Creates an invalidation-queue per data-structure-name.
     */
    private final ConstructorFunction<String, InvalidationQueue<Invalidation>> invalidationQueueConstructor
            = dataStructureName -> new InvalidationQueue<>();

    /**
     * data-structure-name to invalidation-queue mappings.
     */
    private final ConcurrentMap<String, InvalidationQueue<Invalidation>> invalidationQueues = new ConcurrentHashMap<>();

    private final int batchSize;
    private final int batchFrequencySeconds;
    private final UUID nodeShutdownListenerId;
    private final AtomicBoolean runningBackgroundTask = new AtomicBoolean(false);

    public BatchInvalidator(String serviceName, int batchSize, int batchFrequencySeconds,
                            Function<EventRegistration, Boolean> eventFilter, NodeEngine nodeEngine) {
        super(serviceName, eventFilter, nodeEngine);

        this.batchSize = batchSize;
        this.batchFrequencySeconds = batchFrequencySeconds;
        this.nodeShutdownListenerId = registerNodeShutdownListener();
        this.invalidationExecutorName = serviceName + getClass();
    }

    @Override
    protected Invalidation newInvalidation(Data key, String dataStructureName, UUID sourceUuid, int partitionId) {
        if (key != null) {
            // when key is null invalidations are sent
            // immediately hence no need to check background task.
            checkBackgroundTaskIsRunning();
        }
        return super.newInvalidation(key, dataStructureName, sourceUuid, partitionId);
    }

    @Override
    protected void invalidateInternal(Invalidation invalidation, int orderKey) {
        String dataStructureName = invalidation.getName();
        InvalidationQueue<Invalidation> invalidationQueue = invalidationQueueOf(dataStructureName);
        invalidationQueue.offer(invalidation);

        if (invalidationQueue.size() >= batchSize) {
            pollAndSendInvalidations(dataStructureName, invalidationQueue);
        }
    }

    private InvalidationQueue<Invalidation> invalidationQueueOf(String dataStructureName) {
        return getOrPutIfAbsent(invalidationQueues, dataStructureName, invalidationQueueConstructor);
    }

    private void pollAndSendInvalidations(String dataStructureName, InvalidationQueue<Invalidation> invalidationQueue) {
        assert invalidationQueue != null;

        if (!invalidationQueue.tryAcquire()) {
            return;
        }

        List<Invalidation> invalidations;
        try {
            invalidations = pollInvalidations(invalidationQueue);
        } finally {
            invalidationQueue.release();
        }

        sendInvalidations(dataStructureName, invalidations);
    }

    private List<Invalidation> pollInvalidations(InvalidationQueue<Invalidation> invalidationQueue) {
        final int size = invalidationQueue.size();

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

    private void sendInvalidations(String dataStructureName, List<Invalidation> invalidations) {
        // There will always be at least one listener which listens invalidations. This is the reason behind eager creation
        // of BatchNearCacheInvalidation instance here. There is a causality between listener and invalidation. Only if we have
        // a listener, we can have an invalidation, otherwise invalidations are not generated.
        Invalidation invalidation = new BatchNearCacheInvalidation(dataStructureName, invalidations);

        Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, dataStructureName);
        for (EventRegistration registration : registrations) {
            if (eventFilter.apply(registration)) {
                // find worker queue of striped executor by using subscribers' address.
                // we want to send all batch invalidations belonging to same subscriber go into
                // the same workers queue.
                int orderKey = registration.getSubscriber().hashCode();
                eventService.publishEvent(serviceName, registration, invalidation, orderKey);
            }
        }
    }

    /**
     * Sends remaining invalidation events in this invalidator's queues to the recipients.
     */
    private UUID registerNodeShutdownListener() {
        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        return lifecycleService.addLifecycleListener(event -> {
            if (event.getState() == SHUTTING_DOWN) {
                Set<Map.Entry<String, InvalidationQueue<Invalidation>>> entries = invalidationQueues.entrySet();
                for (Map.Entry<String, InvalidationQueue<Invalidation>> entry : entries) {
                    pollAndSendInvalidations(entry.getKey(), entry.getValue());
                }
            }
        });
    }

    private void checkBackgroundTaskIsRunning() {
        if (runningBackgroundTask.get()) {
            // return if already started
            return;
        }

        if (runningBackgroundTask.compareAndSet(false, true)) {
            ExecutionService executionService = nodeEngine.getExecutionService();
            executionService.scheduleWithRepetition(invalidationExecutorName,
                    new BatchInvalidationEventSender(), batchFrequencySeconds, batchFrequencySeconds, SECONDS);
        }
    }

    /**
     * A background runner which runs periodically and consumes invalidation queues.
     */
    private class BatchInvalidationEventSender implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<String, InvalidationQueue<Invalidation>> entry : invalidationQueues.entrySet()) {
                if (currentThread().isInterrupted()) {
                    break;
                }
                String name = entry.getKey();
                InvalidationQueue<Invalidation> invalidationQueue = entry.getValue();
                if (invalidationQueue.size() > 0) {
                    pollAndSendInvalidations(name, invalidationQueue);
                }
            }
        }
    }

    @Override
    public void destroy(String dataStructureName, UUID sourceUuid) {
        invalidationQueues.remove(dataStructureName);
        super.destroy(dataStructureName, sourceUuid);
    }

    @Override
    public void shutdown() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.shutdownExecutor(invalidationExecutorName);

        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        lifecycleService.removeLifecycleListener(nodeShutdownListenerId);

        invalidationQueues.clear();

        super.shutdown();
    }

    @Override
    public void reset() {
        invalidationQueues.clear();

        super.reset();
    }
}
