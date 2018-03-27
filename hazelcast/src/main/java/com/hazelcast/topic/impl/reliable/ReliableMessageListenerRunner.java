/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Message;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.topic.ReliableMessageListener;


/**
 * An {@link com.hazelcast.core.ExecutionCallback} that will try to read an
 * item from the ringbuffer or blocks if no item is available. All data
 * that are read is pushed into the {@link com.hazelcast.core.MessageListener}.
 * It is a self-perpetuating stream of async calls.
 * <p/>
 * The runner keeps track of the sequence.
 */
class ReliableMessageListenerRunner<E> implements ExecutionCallback<ReadResultSet<ReliableTopicMessage>> {

    final ReliableMessageListener<E> listener;
    private final Ringbuffer<ReliableTopicMessage> ringbuffer;
    private final String topicName;
    private final SerializationService serializationService;
    private final ClusterService clusterService;
    private final ILogger logger;
    private final String id;
    private final ReliableTopicProxy<E> proxy;

    private long sequence;
    private volatile boolean cancelled;
    private final int batchSze;

    public ReliableMessageListenerRunner(String id,
                                         ReliableMessageListener<E> listener,
                                         ReliableTopicProxy<E> proxy) {
        this.id = id;
        this.listener = listener;
        this.proxy = proxy;
        this.ringbuffer = proxy.ringbuffer;
        this.topicName = proxy.getName();
        NodeEngine nodeEngine = proxy.getNodeEngine();
        this.serializationService = nodeEngine.getSerializationService();
        this.clusterService = nodeEngine.getClusterService();
        this.logger = nodeEngine.getLogger(ReliableMessageListenerRunner.class);
        this.batchSze = proxy.topicConfig.getReadBatchSize();

        // we are going to listen to next publication. We don't care about what already has been published.
        long initialSequence = listener.retrieveInitialSequence();
        if (initialSequence == -1) {
            initialSequence = ringbuffer.tailSequence() + 1;
        }
        this.sequence = initialSequence;
    }

    void next() {
        if (cancelled) {
            return;
        }

        ICompletableFuture<ReadResultSet<ReliableTopicMessage>> f =
                ringbuffer.readManyAsync(sequence, 1, batchSze, null);
        f.andThen(this, proxy.executor);
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
     * @throws Throwable
     */
    private void process(ReliableTopicMessage message) throws Throwable {
        proxy.localTopicStats.incrementReceives();
        listener.onMessage(toMessage(message));
    }

    private Message<E> toMessage(ReliableTopicMessage m) {
        MemberImpl member = clusterService.getMember(m.getPublisherAddress());
        E payload = serializationService.toObject(m.getPayload());
        return new Message<E>(topicName, payload, m.getPublishTime(), member);
    }

    // This method is called from the provided executor.
    @Override
    public void onFailure(Throwable t) {
        if (cancelled) {
            return;
        }

        if (t instanceof IllegalArgumentException && listener.isLossTolerant()) {
            if (handleIllegalArgumentException((IllegalArgumentException) t)) {
                return;
            }
        } else if (t instanceof StaleSequenceException) {
            if (handleStaleSequenceException((StaleSequenceException) t)) {
                return;
            }
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

        cancel();
    }

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
        if (listener.isLossTolerant()) {
            if (logger.isFinestEnabled()) {
                logger.finest("MessageListener " + listener + " on topic: " + topicName + " ran into a stale sequence. "
                        + "Jumping from oldSequence: " + sequence
                        + " to sequence: " + staleSequenceException.getHeadSeq());
            }
            sequence = staleSequenceException.getHeadSeq();
            next();
            return true;
        }

        logger.warning("Terminating MessageListener:" + listener + " on topic: " + topicName + ". "
                + "Reason: The listener was too slow or the retention period of the message has been violated. "
                + "head: " + staleSequenceException.getHeadSeq() + " sequence:" + sequence);
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
        if (logger.isFinestEnabled()) {
            logger.finest(String.format("MessageListener %s on topic %s requested a too large sequence: %s. "
                            + ". Jumping from old sequence: %s to sequence: %s",
                    listener, topicName, t.getMessage(), sequence, currentHeadSequence));
        }
        this.sequence = currentHeadSequence;
        next();
        return true;
    }

    void cancel() {
        cancelled = true;
        proxy.runnersMap.remove(id);
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
}
