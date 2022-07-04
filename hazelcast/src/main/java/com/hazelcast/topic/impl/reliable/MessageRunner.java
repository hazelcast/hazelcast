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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * An {@link com.hazelcast.core.ExecutionCallback} that will try to read an
 * item from the ringbuffer or blocks if no item is available. All data
 * that are read is pushed into the {@link MessageListener}.
 * It is a self-perpetuating stream of async calls.
 * <p>
 * The runner keeps track of the sequence.
 */
public abstract class MessageRunner<E> implements BiConsumer<ReadResultSet<ReliableTopicMessage>, Throwable> {

    protected final Ringbuffer<ReliableTopicMessage> ringbuffer;
    protected final ILogger logger;
    protected final ReliableMessageListener<E> listener;
    protected final String topicName;
    protected volatile long sequence;
    private final SerializationService serializationService;
    private final ConcurrentMap<UUID, MessageRunner<E>> runnersMap;
    private final UUID id;
    private final Executor executor;
    private final int batchSize;
    private volatile boolean cancelled;

    public MessageRunner(UUID id,
                         ReliableMessageListener<E> listener,
                         Ringbuffer<ReliableTopicMessage> ringbuffer,
                         String topicName,
                         int batchSize,
                         SerializationService serializationService,
                         Executor executor,
                         ConcurrentMap<UUID, MessageRunner<E>> runnersMap,
                         ILogger logger) {
        this.id = id;
        this.listener = listener;
        this.ringbuffer = ringbuffer;
        this.topicName = topicName;
        this.serializationService = serializationService;
        this.logger = logger;
        this.batchSize = batchSize;
        this.executor = executor;
        this.runnersMap = runnersMap;

        // we are going to listen to next publication. We don't care about what already has been published.
        long initialSequence = listener.retrieveInitialSequence();
        if (initialSequence == -1) {
            initialSequence = ringbuffer.tailSequence() + 1;
        }
        this.sequence = initialSequence;
    }

    public void next() {
        if (cancelled) {
            return;
        }
        ringbuffer.readManyAsync(sequence, 1, batchSize, null)
                  .whenCompleteAsync(this, executor);
    }

    @Override
    public void accept(ReadResultSet<ReliableTopicMessage> result, Throwable throwable) {
        if (cancelled) {
            return;
        }

        if (throwable == null) {
            // we process all messages in batch. So we don't release the thread and reschedule ourselves;
            // but we'll process whatever was received in 1 go.

            long lostCount = result.getNextSequenceToReadFrom() - result.readCount() - sequence;
            if (lostCount != 0 && !isLossTolerable(lostCount)) {
                cancel();
                return;
            }

            for (int i = 0; i < result.size(); i++) {
                ReliableTopicMessage message = result.get(i);
                try {
                    listener.storeSequence(result.getSequence(i));
                    listener.onMessage(toMessage(message));
                } catch (Throwable t) {
                    if (terminate(t)) {
                        cancel();
                        return;
                    }
                }

            }

            sequence = result.getNextSequenceToReadFrom();
            next();
        } else {
            throwable = adjustThrowable(throwable);
            if (handleInternalException(throwable)) {
                next();
            } else {
                cancel();
            }
        }
    }

    private Message<E> toMessage(ReliableTopicMessage m) {
        Member member = getMember(m);
        E payload = serializationService.toObject(m.getPayload());
        return new Message<E>(topicName, payload, m.getPublishTime(), member);
    }

    protected abstract Member getMember(ReliableTopicMessage m);

    /**
     * @param t throwable to check if it is terminal or can be handled so that topic can continue
     * @return true if the exception was handled and the listener may continue reading
     */
    protected boolean handleInternalException(Throwable t) {
        if (t instanceof OperationTimeoutException) {
            return handleOperationTimeoutException();
        } else if (t instanceof IllegalArgumentException) {
            return handleIllegalArgumentException((IllegalArgumentException) t);
        } else if (t instanceof HazelcastInstanceNotActiveException) {
            if (logger.isFinestEnabled()) {
                logger.finest("Terminating MessageListener " + listener + " on topic: " + topicName + ". "
                        + " Reason: HazelcastInstance is shutting down");
            }
        } else if (t instanceof DistributedObjectDestroyedException) {
            if (logger.isFinestEnabled()) {
                logger.finest("Terminating MessageListener " + listener + " on topic: " + topicName + ". "
                        + "Reason: Topic is destroyed");
            }
        } else {
            logger.warning("Terminating MessageListener " + listener + " on topic: " + topicName + ". "
                    + "Reason: Unhandled exception, message: " + t.getMessage(), t);
        }
        return false;
    }

    private boolean handleOperationTimeoutException() {
        if (logger.isFinestEnabled()) {
            logger.finest("MessageListener " + listener + " on topic: " + topicName + " timed out. "
                    + "Continuing from last known sequence: " + sequence);
        }
        return true;
    }

    /**
     * Needed because client / server behaviour is different on onFailure call
     *
     * @param t throwable
     * @return adjusted throwable
     */
    protected abstract Throwable adjustThrowable(Throwable t);

    /**
     * Called when message loss is detected. Checks if the listener is able
     * to tolerate the loss.
     *
     * @param lossCount number of lost messages
     * @return if the listener may continue reading
     */
    private boolean isLossTolerable(long lossCount) {
        if (listener.isLossTolerant()) {
            if (logger.isFinestEnabled()) {
                logger.finest("MessageListener " + listener + " on topic: " + topicName + " lost " + lossCount
                        + "messages");
            }
            return true;
        }

        logger.warning("Terminating MessageListener:" + listener + " on topic: " + topicName + ". "
                + "Reason: The listener was too slow or the retention period of the message has been violated. "
                + lossCount + " messages lost.");
        return false;
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
        if (listener.isLossTolerant()) {
            if (logger.isFinestEnabled()) {
                logger.finest(String.format("MessageListener %s on topic %s requested a too large sequence: %s. "
                                + ". Jumping from old sequence: %s to sequence: %s",
                        listener, topicName, t.getMessage(), sequence, currentHeadSequence));
            }
            this.sequence = currentHeadSequence;
            return true;
        }

        logger.warning("Terminating MessageListener:" + listener + " on topic: " + topicName + ". "
                + "Reason: Underlying ring buffer data related to reliable topic is lost. ");
        return false;
    }

    public void cancel() {
        cancelled = true;
        runnersMap.remove(id);
    }

    private boolean terminate(Throwable failure) {
        if (cancelled) {
            return true;
        }

        try {
            boolean terminate = listener.isTerminal(failure);
            if (terminate) {
                logger.warning("Terminating MessageListener " + listener + " on topic: " + topicName + ". "
                        + "Reason: Unhandled exception, message: " + failure.getMessage(), failure);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("MessageListener " + listener + " on topic: " + topicName + " ran into an exception:"
                            + " message:" + failure.getMessage(), failure);
                }
            }
            return terminate;
        } catch (Throwable t) {
            logger.warning("Terminating messageListener:" + listener + " on topic: " + topicName + ". "
                    + "Reason: Unhandled exception while calling ReliableMessageListener.isTerminal() method", t);
            return true;
        }
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
