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

package com.hazelcast.client.proxy;

import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.topic.impl.reliable.ReliableTopicService.SERVICE_NAME;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClientReliableTopicProxy<E> extends ClientProxy implements ITopic<E> {
    public static final int MAX_BACKOFF = 2000;
    public static final int INITIAL_BACKOFF_MS = 100;

    protected final ILogger logger = Logger.getLogger(getClass());

    private final ConcurrentMap<String, MessageRunner> runnersMap
            = new ConcurrentHashMap<String, MessageRunner>();

    private final Ringbuffer ringbuffer;
    private final SerializationService serializationService;
    private final ClientReliableTopicConfig config;
    private final Executor executor;
    private final TopicOverloadPolicy overloadPolicy;

    public ClientReliableTopicProxy(String objectId, HazelcastClientInstanceImpl client) {
        super(SERVICE_NAME, objectId);
        this.ringbuffer = client.getRingbuffer(TOPIC_RB_PREFIX + objectId);
        this.serializationService = client.getSerializationService();

        this.config = client.getClientConfig().getReliableTopicConfig(objectId);
        this.executor = getExecutor(config, client);
        this.overloadPolicy = config.getTopicOverloadPolicy();
    }

    private Executor getExecutor(ClientReliableTopicConfig config, HazelcastClientInstanceImpl client) {
        Executor executor = config.getExecutor();
        if (executor == null) {
            executor = client.getClientExecutionService();
        }
        return executor;
    }

    @Override
    public void publish(E payload) {
        try {
            Data data = serializationService.toData(payload);
            ReliableTopicMessage message = new ReliableTopicMessage(data, null);
            switch (overloadPolicy) {
                case ERROR:
                    addOrFail(message);
                    break;
                case DISCARD_OLDEST:
                    addOrOverwrite(message);
                    break;
                case DISCARD_NEWEST:
                    ringbuffer.addAsync(message, OverflowPolicy.FAIL).get();
                    break;
                case BLOCK:
                    addWithBackoff(message);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new HazelcastException("Failed to publish message: " + payload + " to topic:" + name, e);
        }
    }

    private Long addOrOverwrite(ReliableTopicMessage message) throws Exception {
        return (Long) ringbuffer.addAsync(message, OverflowPolicy.OVERWRITE).get();
    }

    private void addOrFail(ReliableTopicMessage message) throws Exception {
        long sequenceId = (Long) ringbuffer.addAsync(message, OverflowPolicy.FAIL).get();
        if (sequenceId == -1) {
            throw new TopicOverloadException("Failed to publish message: " + message + " on topic:" + name);
        }
    }

    private void addWithBackoff(ReliableTopicMessage message) throws Exception {
        long timeoutMs = INITIAL_BACKOFF_MS;
        for (; ; ) {
            long result = (Long) ringbuffer.addAsync(message, OverflowPolicy.FAIL).get();
            if (result != -1) {
                break;
            }

            MILLISECONDS.sleep(timeoutMs);
            timeoutMs *= 2;
            if (timeoutMs > MAX_BACKOFF) {
                timeoutMs = MAX_BACKOFF;
            }
        }
    }

    @Override
    public String addMessageListener(MessageListener<E> listener) {
        checkNotNull(listener, "listener can't be null");

        String id = UUID.randomUUID().toString();
        ReliableMessageListener<E> reliableMessageListener = toReliableMessageListener(listener);

        MessageRunner runner = new MessageRunner(id, reliableMessageListener);
        runnersMap.put(id, runner);
        runner.next();
        return id;
    }

    private ReliableMessageListener<E> toReliableMessageListener(MessageListener<E> listener) {
        if (listener instanceof ReliableMessageListener) {
            return (ReliableMessageListener) listener;
        } else {
            return new ReliableMessageListenerAdapter<E>(listener);
        }
    }

    @Override
    public boolean removeMessageListener(String registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");

        MessageRunner runner = runnersMap.get(registrationId);
        if (runner == null) {
            return false;
        }
        runner.cancel();
        return true;
    }

    @Override
    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public String toString() {
        return "ITopic{" + "name='" + name + '\'' + '}';
    }

    class MessageRunner implements ExecutionCallback<ReadResultSet<ReliableTopicMessage>> {

        final ReliableMessageListener<E> listener;
        private final String id;
        private long sequence;
        private volatile boolean cancelled;

        public MessageRunner(String id, ReliableMessageListener<E> listener) {
            this.id = id;
            this.listener = listener;

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

            ICompletableFuture<ReadResultSet<ReliableTopicMessage>> f
                    = ringbuffer.readManyAsync(sequence, 1, config.getReadBatchSize(), null);
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

        private void process(ReliableTopicMessage message) throws Throwable {
            //  proxy.localTopicStats.incrementReceives();
            listener.onMessage(toMessage(message));
        }

        private Message<E> toMessage(ReliableTopicMessage m) {
            Member member = null;
            if (m.getPublisherAddress() != null) {
                member = new com.hazelcast.client.impl.MemberImpl(m.getPublisherAddress());
            }
            E payload = serializationService.toObject(m.getPayload());
            return new Message<E>(name, payload, m.getPublishTime(), member);
        }

        // This method is called from the provided executor.
        @Override
        public void onFailure(Throwable t) {
            if (cancelled) {
                return;
            }

            if (t instanceof StaleSequenceException) {
                StaleSequenceException staleSequenceException = (StaleSequenceException) t;

                if (listener.isLossTolerant()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("MessageListener " + listener + " on topic: " + name + " ran into a stale sequence. "
                                + "Jumping from oldSequence: " + sequence
                                + " to sequence: " + staleSequenceException.getHeadSeq());
                    }
                    sequence = staleSequenceException.getHeadSeq();
                    next();
                    return;
                }

                logger.warning("Terminating MessageListener:" + listener + " on topic: " + name + ". "
                        + "Reason: The listener was too slow or the retention period of the message has been violated. "
                        + "head: " + staleSequenceException.getHeadSeq() + " sequence:" + sequence);
            } else if (t instanceof HazelcastInstanceNotActiveException) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Terminating MessageListener " + listener + " on topic: " + name + ". "
                            + " Reason: HazelcastInstance is shutting down");
                }
            } else if (t instanceof DistributedObjectDestroyedException) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Terminating MessageListener " + listener + " on topic: " + name + ". "
                            + "Reason: Topic is destroyed");
                }
            } else {
                logger.warning("Terminating MessageListener " + listener + " on topic: " + name + ". "
                        + "Reason: Unhandled exception, message: " + t.getMessage(), t);
            }

            cancel();
        }

        void cancel() {
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
                    logger.warning("Terminating MessageListener " + listener + " on topic: " + name + ". "
                            + "Reason: Unhandled exception, message: " + failure.getMessage(), failure);
                } else {
                    if (logger.isFinestEnabled()) {
                        logger.finest("MessageListener " + listener + " on topic: " + name + " ran into an exception:"
                                + " message:" + failure.getMessage(), failure);
                    }
                }
                return terminate;
            } catch (Throwable t) {
                logger.warning("Terminating messageListener:" + listener + " on topic: " + name + ". "
                        + "Reason: Unhandled exception while calling ReliableMessageListener.isTerminal() method", t);
                return true;
            }
        }
    }
}
