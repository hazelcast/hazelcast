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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The serverside {@link ITopic} implementation for reliable topics.
 *
 * @param <E> type of item contained in the topic
 */
public class ReliableTopicProxy<E> extends AbstractDistributedObject<ReliableTopicService> implements ITopic<E> {

    public static final int MAX_BACKOFF = 2000;
    public static final int INITIAL_BACKOFF_MS = 100;
    private static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    final Ringbuffer<ReliableTopicMessage> ringbuffer;
    final Executor executor;
    final ConcurrentMap<UUID, MessageRunner<E>> runnersMap
            = new ConcurrentHashMap<UUID, MessageRunner<E>>();

    /**
     * Local statistics for this reliable topic, including
     * messages received on and published through this topic.
     */
    final LocalTopicStatsImpl localTopicStats;
    final ReliableTopicConfig topicConfig;
    final TopicOverloadPolicy overloadPolicy;

    private final NodeEngine nodeEngine;
    private final Address thisAddress;
    private final String name;

    public ReliableTopicProxy(String name, NodeEngine nodeEngine, ReliableTopicService service,
                              ReliableTopicConfig topicConfig) {
        super(nodeEngine, service);

        this.name = name;
        this.topicConfig = topicConfig;
        this.nodeEngine = nodeEngine;
        this.ringbuffer = nodeEngine.getHazelcastInstance().getRingbuffer(TOPIC_RB_PREFIX + name);
        this.executor = initExecutor(nodeEngine, topicConfig);
        this.thisAddress = nodeEngine.getThisAddress();
        this.overloadPolicy = topicConfig.getTopicOverloadPolicy();
        this.localTopicStats = service.getLocalTopicStats(name);

        for (ListenerConfig listenerConfig : topicConfig.getMessageListenerConfigs()) {
            addMessageListener(listenerConfig);
        }
    }

    @Override
    public String getServiceName() {
        return ReliableTopicService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    private void addMessageListener(ListenerConfig listenerConfig) {
        NodeEngine nodeEngine = getNodeEngine();

        MessageListener listener = loadListener(listenerConfig);

        if (listener == null) {
            return;
        }

        if (listener instanceof HazelcastInstanceAware) {
            HazelcastInstanceAware hazelcastInstanceAware = (HazelcastInstanceAware) listener;
            hazelcastInstanceAware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
        addMessageListener(listener);
    }

    private MessageListener loadListener(ListenerConfig listenerConfig) {
        try {
            MessageListener listener = (MessageListener) listenerConfig.getImplementation();
            if (listener != null) {
                return listener;
            }

            if (listenerConfig.getClassName() != null) {
                Object object = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());

                if (!(object instanceof MessageListener)) {
                    throw new HazelcastException("class '"
                            + listenerConfig.getClassName() + "' is not an instance of "
                            + MessageListener.class.getName());
                }
                listener = (MessageListener) object;
            }
            return listener;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Executor initExecutor(NodeEngine nodeEngine, ReliableTopicConfig topicConfig) {
        Executor executor = topicConfig.getExecutor();
        if (executor == null) {
            executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        }
        return executor;
    }

    @Override
    public void publish(@Nonnull E payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
        try {
            Data data = nodeEngine.toData(payload);
            ReliableTopicMessage message = new ReliableTopicMessage(data, thisAddress);
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

            localTopicStats.incrementPublishes();
        } catch (Exception e) {
            throw (RuntimeException) peel(e, null,
                    "Failed to publish message: " + payload + " to topic:" + getName());
        }
    }

    private Long addOrOverwrite(ReliableTopicMessage message) throws Exception {
        return ringbuffer.addAsync(message, OverflowPolicy.OVERWRITE).toCompletableFuture().get();
    }

    private void addOrFail(ReliableTopicMessage message) throws Exception {
        long sequenceId = ringbuffer.addAsync(message, OverflowPolicy.FAIL).toCompletableFuture().get();
        if (sequenceId == -1) {
            throw new TopicOverloadException("Failed to publish message: " + message + " on topic:" + getName());
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
        ReliableMessageListener<E> reliableMessageListener;
        if (listener instanceof ReliableMessageListener) {
            reliableMessageListener = (ReliableMessageListener) listener;
        } else {
            reliableMessageListener = new ReliableMessageListenerAdapter<E>(listener);
        }

        MessageRunner<E> runner = new ReliableMessageRunner<E>(id, reliableMessageListener,
                nodeEngine.getSerializationService(), executor, nodeEngine.getLogger(this.getClass()),
                nodeEngine.getClusterService(), this);
        runnersMap.put(id, runner);
        runner.next();
        return id;
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

    @Override
    protected void postDestroy() {
        // this will trigger all listeners to destroy themselves.
        ringbuffer.destroy();
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        return localTopicStats;
    }
}
