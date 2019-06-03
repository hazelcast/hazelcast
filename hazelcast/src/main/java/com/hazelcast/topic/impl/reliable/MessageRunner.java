/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.topic.ReliableMessageListener;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;


/**
 * An {@link com.hazelcast.core.ExecutionCallback} that will try to read an
 * item from the ringbuffer or blocks if no item is available. All data
 * that are read is pushed into the {@link com.hazelcast.core.MessageListener}.
 * It is a self-perpetuating stream of async calls.
 * <p>
 * The runner keeps track of the sequence.
 */
public abstract class MessageRunner<E> implements ExecutionCallback<ReadResultSet<ReliableTopicMessage>> {

    protected final Ringbuffer<ReliableTopicMessage> ringbuffer;
    protected final ILogger logger;
    protected final ReliableMessageListener<E> listener;
    protected final String topicName;
    protected long sequence;
    private final SerializationService serializationService;
    private final ConcurrentMap<String, MessageRunner<E>> runnersMap;
    private final String id;
    private final Executor executor;
    private final int batchSze;
    private volatile boolean cancelled;

    public MessageRunner(String id,
                         ReliableMessageListener<E> listener,
                         Ringbuffer<ReliableTopicMessage> ringbuffer,
                         String topicName,
                         int batchSze,
                         SerializationService serializationService,
                         Executor executor,
                         ConcurrentMap<String, MessageRunner<E>> runnersMap,
                         ILogger logger) {
        this.id = id;
        this.listener = listener;
        this.ringbuffer = ringbuffer;
        this.topicName = topicName;
        this.serializationService = serializationService;
        this.logger = logger;
        this.batchSze = batchSze;
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

        ICompletableFuture<ReadResultSet<ReliableTopicMessage>> f =
                ringbuffer.readManyAsync(sequence, 1, batchSze, null);
        f.andThen(this, executor);
    }

    // This method is called from the provided executor.
    @Override
    public void onResponse(ReadResultSet<ReliableTopicMessage> result) {
        // we process all messages in batch. So we don't release the thread and reschedule ourselves;
        // but we'll process whatever was received in 1 go.
        for (Object item : result) {
            ReliableTopicMessage message = (ReliableTopicMessage) item;

            if (cancelled) {
                return;
            }

            try {
                listener.storeSequence(sequence);
                process(message);
            } catch (Throwable t) {
                if (terminate(t)) {
                    cancel();
                    return;
                }
            }

            sequence++;
        }
        next();
    }

    /**
     * Processes the message by increasing the local topic stats and
     * calling the user supplied listener.
     *
     * @param message the reliable topic message
     */
    private void process(ReliableTopicMessage message) {
        updateStatistics();
        listener.onMessage(toMessage(message));
    }

    protected abstract void updateStatistics();

    private Message<E> toMessage(ReliableTopicMessage m) {
        Member member = getMember(m);
        E payload = serializationService.toObject(m.getPayload());
        return new Message<E>(topicName, payload, m.getPublishTime(), member);
    }

    protected abstract Member getMember(ReliableTopicMessage m);

    // This method is called from the provided executor.
    @Override
    public void onFailure(Throwable t) {
        if (cancelled) {
            return;
        }

        t = adjustThrowable(t);
        if (handleInternalException(t)) {
            next();
        } else {
            cancel();
        }
    }

    /**
     * @param t throwable to check if it is terminal or can be handled so that topic can continue
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
     * Handles a {@link StaleSequenceException} associated with requesting
     * a sequence older than the {@code headSequence}.
     * This may indicate that the reader was too slow and items in the
     * ringbuffer were already overwritten.
     *
     * @param staleSequenceException the exception
     * @return if the exception was handled and the listener may continue reading
     */
    private boolean handleStaleSequenceException(StaleSequenceException staleSequenceException) {
        long headSeq = getHeadSequence(staleSequenceException);
        if (listener.isLossTolerant()) {
            if (logger.isFinestEnabled()) {
                logger.finest("MessageListener " + listener + " on topic: " + topicName + " ran into a stale sequence. "
                        + "Jumping from oldSequence: " + sequence
                        + " to sequence: " + headSeq);
            }
            sequence = headSeq;
            return true;
        }

        logger.warning("Terminating MessageListener:" + listener + " on topic: " + topicName + ". "
                + "Reason: The listener was too slow or the retention period of the message has been violated. "
                + "head: " + headSeq + " sequence:" + sequence);
        return false;
    }

    protected abstract long getHeadSequence(StaleSequenceException staleSequenceException);

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
