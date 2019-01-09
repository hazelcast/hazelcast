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

package com.hazelcast.client.proxy;

import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.MessageRunner;
import com.hazelcast.topic.impl.reliable.ReliableMessageListenerAdapter;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;
import com.hazelcast.util.UuidUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.hazelcast.client.proxy.ClientMapProxy.NULL_LISTENER_IS_NOT_ALLOWED;
import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.topic.impl.reliable.ReliableTopicService.SERVICE_NAME;
import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.Preconditions.checkNotNull;
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

    private static final int MAX_BACKOFF = 2000;
    private static final int INITIAL_BACKOFF_MS = 100;

    private final ILogger logger;
    private final ConcurrentMap<String, MessageRunner<E>> runnersMap = new ConcurrentHashMap<String, MessageRunner<E>>();
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
            executor = client.getClientExecutionService().getUserExecutor();
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
        } catch (Exception e) {
            throw (RuntimeException) peel(e, null,
                    "Failed to publish message: " + payload + " to topic:" + getName());
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
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);

        String id = UuidUtil.newUnsecureUuidString();
        ReliableMessageListener<E> reliableMessageListener = toReliableMessageListener(listener);

        MessageRunner<E> runner = new ClientReliableMessageRunner<E>(id, reliableMessageListener,
                ringbuffer, name, config.getReadBatchSize(),
                serializationService, executor, runnersMap, logger);
        runnersMap.put(id, runner);
        runner.next();
        return id;
    }

    //for testing
    public boolean isListenerCancelled(String registrationID) {
        checkNotNull(registrationID, "registrationId can't be null");

        MessageRunner runner = runnersMap.get(registrationID);
        if (runner == null) {
            return true;
        }
        return runner.isCancelled();
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
