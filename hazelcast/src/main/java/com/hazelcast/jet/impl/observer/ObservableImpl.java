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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.Observer;
import com.hazelcast.jet.impl.execution.DoneItem;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.RingbufferProxy;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

public class ObservableImpl<T> implements Observable<T> {

    /**
     * Prefix of all topic names used to back {@link Observable} implementations,
     * necessary so that such topics can't clash with regular topics used
     * for other purposes.
     */
    public static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observables.";

    /**
     * Constant ID to be used as a {@link ProcessorMetaSupplier#getTags()
     * PMS tag key} for specifying when a PMS owns an {@link Observable} (i.e.
     * is the entity populating the {@link Observable} with data).
     */
    public static final String OWNED_OBSERVABLE = ObservableImpl.class.getName() + ".ownedObservable";

    private final ConcurrentMap<UUID, RingbufferListener<T>> listeners = new ConcurrentHashMap<>();
    private final String name;
    private final HazelcastInstance hzInstance;
    private final Consumer<Observable<T>> onDestroy;
    private final ILogger logger;

    public ObservableImpl(String name, HazelcastInstance hzInstance, Consumer<Observable<T>> onDestroy, ILogger logger) {
        this.name = name;
        this.hzInstance = hzInstance;
        this.onDestroy = onDestroy;
        this.logger = logger;
    }

    @Nonnull @Override
    public String name() {
        return name;
    }

    @Nonnull @Override
    public UUID addObserver(@Nonnull Observer<T> observer) {
        UUID id = UuidUtil.newUnsecureUUID();
        RingbufferListener<T> listener = new RingbufferListener<>(name, id, observer, hzInstance, logger);
        listeners.put(id, listener);
        listener.next();
        return id;
    }

    @Override
    public void removeObserver(@Nonnull UUID registrationId) {
        RingbufferListener<T> listener = listeners.remove(registrationId);
        if (listener == null) {
            throw new IllegalArgumentException(
                    String.format("No registered observer with registration ID %s", registrationId));
        } else {
            listener.cancel();
        }
    }

    @Override
    public Observable<T> configureCapacity(int capacity) {
        String ringbufferName = ringbufferName(name);
        if (ringbufferExists(ringbufferName)) {
            throw new IllegalStateException("Underlying buffer for observable '" + name + "' is already created.");
        }
        Config config = hzInstance.getConfig();
        try {
            config.addRingBufferConfig(new RingbufferConfig(ringbufferName).setCapacity(capacity));
        } catch (Exception e) {
            throw new RuntimeException("Failed configuring capacity: " + e, e);
        }
        return this;
    }

    @Override
    public int getConfiguredCapacity() {
        String ringbufferName = ringbufferName(name);
        if (ringbufferExists(ringbufferName)) {
            return (int) hzInstance.getRingbuffer(ringbufferName).capacity();
        } else {
            //theoretically we could read the default config's capacity, but that's not possible
            //when the HazelcastInstance is just a client
            throw new IllegalStateException("Underlying buffer for observable '" + name + "' is not yet created.");
        }
    }

    private boolean ringbufferExists(String ringbufferName) {
        return hzInstance.getDistributedObjects().stream().anyMatch(
                o -> o.getServiceName().equals(RingbufferService.SERVICE_NAME) && o.getName().equals(ringbufferName));
    }

    @Override
    public void destroy() {
        listeners.keySet().forEach(this::removeObserver);

        // destroy the underlying ringbuffer
        hzInstance.getRingbuffer(ringbufferName(name)).destroy();
        onDestroy.accept(this);
    }

    @Nonnull
    public static String ringbufferName(String observableName) {
        return JET_OBSERVABLE_NAME_PREFIX + observableName;
    }

    private static class RingbufferListener<T> {

        private static final int BATCH_SIZE = RingbufferProxy.MAX_BATCH_SIZE;

        private final String id;
        private final Observer<T> observer;
        private final Ringbuffer<Object> ringbuffer;
        private final ILogger logger;
        private final Executor executor;

        private long sequence;
        private volatile boolean cancelled;

        RingbufferListener(
                String observable,
                UUID uuid,
                Observer<T> observer,
                HazelcastInstance hzInstance,
                ILogger logger
        ) {
            this.observer = observer;
            this.ringbuffer = hzInstance.getRingbuffer(ringbufferName(observable));
            this.id = uuid.toString() + "/" + ringbuffer.getName();
            this.executor = getExecutor(hzInstance);
            this.sequence = ringbuffer.headSequence();
            this.logger = logger;

            this.logger.info("Starting message listener '" + id + "'");
        }

        private static Executor getExecutor(HazelcastInstance hzInstance) {
            if (hzInstance instanceof HazelcastInstanceImpl) {
                return ((HazelcastInstanceImpl) hzInstance).node.getNodeEngine().getExecutionService()
                        .getExecutor(ExecutionService.ASYNC_EXECUTOR);
            } else if (hzInstance instanceof HazelcastClientInstanceImpl) {
                return ((HazelcastClientInstanceImpl) hzInstance).getTaskScheduler();
            } else {
                throw new RuntimeException(String.format("Unhandled %s type: %s", HazelcastInstance.class.getSimpleName(),
                        hzInstance.getClass().getName()));
            }
        }

        void next() {
            if (cancelled) {
                return;
            }
            ringbuffer.readManyAsync(sequence, 1, BATCH_SIZE, null)
                    .whenCompleteAsync(this::accept, executor);
        }

        void cancel() {
            cancelled = true;
        }

        private void accept(ReadResultSet<Object> result, Throwable throwable) {
            if (cancelled) {
                return;
            }
            if (throwable == null) {
                // we process all messages in batch. So we don't release the thread and reschedule ourselves;
                // but we'll process whatever was received in 1 go.

                long lostCount = result.getNextSequenceToReadFrom() - result.readCount() - sequence;
                if (lostCount != 0) {
                    logger.warning(String.format("Message loss of %d messages detected in listener '%s'", lostCount, id));
                }

                for (int i = 0; i < result.size(); i++) {
                    try {
                        if (cancelled) {
                            return;
                        }
                        onNewMessage(result.get(i));
                    } catch (Throwable t) {
                        logger.warning("Terminating message listener '" + id + "'. " +
                                "Reason: Unhandled exception, message: " + t.getMessage(), t);
                        cancel();
                        return;
                    }

                }

                sequence = result.getNextSequenceToReadFrom();
                next();
            } else {
                if (handleInternalException(throwable)) {
                    next();
                } else {
                    cancel();
                }
            }
        }

        private void onNewMessage(Object message) {
            try {
                if (message instanceof WrappedThrowable) {
                    observer.onError(((WrappedThrowable) message).get());
                } else if (message instanceof DoneItem) {
                    observer.onComplete();
                } else {
                    //noinspection unchecked
                    observer.onNext((T) message);
                }
            } catch (Throwable t) {
                logger.warning("Exception thrown while calling observer callback for listener '" + id + "'. Will be " +
                        "ignored. Reason: " + t.getMessage(), t);
            }
        }

        /**
         * @param t throwable to check if it is terminal or can be handled so that listening can continue
         * @return true if the exception was handled and the listener may continue reading
         */
        protected boolean handleInternalException(Throwable t) {
            if (t instanceof OperationTimeoutException) {
                return handleOperationTimeoutException();
            } else if (t instanceof IllegalArgumentException) {
                return handleIllegalArgumentException((IllegalArgumentException) t);
            } else if (t instanceof StaleSequenceException) {
                return handleStaleSequenceException((StaleSequenceException) t);
            } else if (t instanceof HazelcastInstanceNotActiveException) {
                logFine(logger, "Terminating message listener '%s'. Reason: HazelcastInstance is shutting down", id);
            } else if (t instanceof HazelcastClientNotActiveException) {
                logFine(logger, "Terminating message listener '%s'. Reason: HazelcastClient is shutting down", id);
            } else if (t instanceof DistributedObjectDestroyedException) {
                logFine(logger, "Terminating message listener '%s'. Reason: Topic is destroyed", id);
            } else {
                logger.warning("Terminating message listener '" + id + "'. " +
                        "Reason: Unhandled exception, message: " + t.getMessage(), t);
            }
            return false;
        }

        private boolean handleOperationTimeoutException() {
            logFine(logger, "Message listener '%s' timed out. Continuing from last known sequence: %d", id, sequence);
            return true;
        }

        /**
         * Handles the {@link IllegalArgumentException} associated with requesting
         * a sequence larger than the {@code tailSequence + 1}.
         * This may indicate that an entire partition or an entire ringbuffer was
         * lost.
         *
         * @param t the exception
         * @return if the exception was handled and the listener may continue reading
         */
        private boolean handleIllegalArgumentException(IllegalArgumentException t) {
            final long currentHeadSequence = ringbuffer.headSequence();
            logFine(logger, "Message listener '%s' requested a too large sequence: %s. Jumping from old " +
                    "sequence %d to sequence %d.", id, t.getMessage(), sequence, currentHeadSequence);
            adjustSequence(currentHeadSequence);
            return true;
        }

        /**
         * Handles a {@link StaleSequenceException} associated with requesting
         * a sequence older than the {@code headSequence}. This should not
         * happen with latest ringbuffer implementation, but keeping it here
         * to be safe.
         *
         * @param staleSequenceException the exception
         * @return if the exception was handled and the listener may continue reading
         */
        private boolean handleStaleSequenceException(StaleSequenceException staleSequenceException) {
            long headSeq = ringbuffer.headSequence();
            logFine(logger, "Message listener '%s' ran into a stale sequence. Jumping from oldSequence %d to " +
                    "sequence %d.", id, sequence, headSeq);
            adjustSequence(headSeq);
            return true;
        }

        private void adjustSequence(long newSequence) {
            if (newSequence > sequence) {
                logger.warning(String.format("Message loss of %d messages detected in listener '%s'",
                        newSequence - sequence, id));
            }
            this.sequence = newSequence;
        }
    }

}
