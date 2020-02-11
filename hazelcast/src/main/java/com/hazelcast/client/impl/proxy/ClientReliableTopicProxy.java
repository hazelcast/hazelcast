/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.MessageRunner;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ConcurrencyUtil.DEFAULT_ASYNC_EXECUTOR;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.topic.impl.reliable.ReliableTopicService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Reliable proxy implementation of {@link ITopic}.
 * <p/>
 * Unlike other topics, a reliable topic has its own {@link com.hazelcast.ringbuffer.Ringbuffer} to store events and
 * has its own executor to process events.
 *
 * @param <E> message type
 */
public class ClientReliableTopicProxy<E> extends ClientProxy implements ITopic<E> {

    private static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    private static final int MAX_BACKOFF = 2000;
    private static final int INITIAL_BACKOFF_MS = 100;

    private final ILogger logger;
    private final ConcurrentMap<UUID, MessageRunner<E>> runnersMap = new ConcurrentHashMap<UUID, MessageRunner<E>>();
    private final Ringbuffer<ReliableTopicMessage> ringbuffer;
    private final SerializationService serializationService;
    private final ClientReliableTopicConfig config;
    private final Executor executor;
    private final TopicOverloadPolicy overloadPolicy;

    public ClientReliableTopicProxy(String objectId, ClientContext context, HazelcastClientInstanceImpl client) {
        super(SERVICE_NAME, objectId, context);
        this.ringbuffer = client.getRingbuffer(TOPIC_RB_PREFIX + objectId);
        this.serializationService = client.getSerializationService();
        this.config = client.getClientConfig().getReliableTopicConfig(objectId);
        this.executor = getExecutor(config, client);
        this.overloadPolicy = config.getTopicOverloadPolicy();
        logger = client.getLoggingService().getLogger(getClass());
    }

    private Executor getExecutor(ClientReliableTopicConfig config, HazelcastClientInstanceImpl client) {
        Executor executor = config.getExecutor();
        if (executor == null) {
            executor = DEFAULT_ASYNC_EXECUTOR;
        }
        return executor;
    }

    @Override
    public void publish(@Nonnull E payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
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
                    ringbuffer.addAsync(message, OverflowPolicy.FAIL).toCompletableFuture().get();
                    break;
                case BLOCK:
                    addWithBackoff(message);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy);
            }
        } catch (Exception e) {
            throw (RuntimeException) peel(e, null,
                    "Failed to publish message: " + payload + " to topic:" + getName());
        }
    }

    private void addOrOverwrite(ReliableTopicMessage message) throws Exception {
        ringbuffer.addAsync(message, OverflowPolicy.OVERWRITE).toCompletableFuture().get();
    }

    private void addOrFail(ReliableTopicMessage message) throws Exception {
        long sequenceId = ringbuffer.addAsync(message, OverflowPolicy.FAIL).toCompletableFuture().get();
        if (sequenceId == -1) {
            throw new TopicOverloadException("Failed to publish message: " + message + " on topic:" + name);
        }
    }

    private void addWithBackoff(ReliableTopicMessage message) throws Exception {
        long timeoutMs = INITIAL_BACKOFF_MS;
        for (; ; ) {
            long result = ringbuffer.addAsync(message, OverflowPolicy.FAIL).toCompletableFuture().get();
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

    @Nonnull
    @Override
    public UUID addMessageListener(@Nonnull MessageListener<E> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        UUID id = UuidUtil.newUnsecureUUID();
        ReliableMessageListener<E> reliableMessageListener = toReliableMessageListener(listener);

        MessageRunner<E> runner = new ClientReliableMessageRunner<>(id, reliableMessageListener,
                ringbuffer, name, config.getReadBatchSize(),
                serializationService, executor, runnersMap, logger);
        runnersMap.put(id, runner);
        runner.next();
        return id;
    }

    //for testing
    public boolean isListenerCancelled(@Nonnull UUID registrationID) {
        checkNotNull(registrationID, "registrationId can't be null");
        MessageRunner<?> runner = runnersMap.get(registrationID);
        if (runner == null) {
            return true;
        }
        return runner.isCancelled();
    }

    private ReliableMessageListener<E> toReliableMessageListener(MessageListener<E> listener) {
        if (listener instanceof ReliableMessageListener) {
            return (ReliableMessageListener<E>) listener;
        } else {
            return new ReliableMessageListenerAdapter<>(listener);
        }
    }

    @Override
    public boolean removeMessageListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId can't be null");

        MessageRunner runner = runnersMap.get(registrationId);
        if (runner == null) {
            return false;
        }
        runner.cancel();
        return true;
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    public Ringbuffer getRingbuffer() {
        return ringbuffer;
    }

    @Override
    public String toString() {
        return "ITopic{" + "name='" + name + '\'' + '}';
    }

    @Override
    protected void postDestroy() {
        // this will trigger all listeners to destroy themselves.
        ringbuffer.destroy();
    }
}
