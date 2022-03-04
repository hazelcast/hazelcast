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

package com.hazelcast.client.config;

import com.hazelcast.client.impl.proxy.ClientReliableTopicProxy;
import com.hazelcast.config.NamedConfig;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.util.concurrent.Executor;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.topic.TopicOverloadPolicy.BLOCK;

/**
 * Contains the ReliableTopic configuration for a client.
 *
 * @see ClientReliableTopicProxy
 */
public class ClientReliableTopicConfig implements NamedConfig {
    /**
     * The default read batch size.
     */
    public static final int DEFAULT_READ_BATCH_SIZE = 10;

    /**
     * The default slow consumer policy.
     */
    public static final TopicOverloadPolicy DEFAULT_TOPIC_OVERLOAD_POLICY = BLOCK;

    private Executor executor;
    private int readBatchSize = DEFAULT_READ_BATCH_SIZE;
    private String name;
    private TopicOverloadPolicy topicOverloadPolicy = DEFAULT_TOPIC_OVERLOAD_POLICY;


    // for spring-instantiation
    public ClientReliableTopicConfig() {
    }

    /**
     * Creates a new ReliableTopicConfig with default settings.
     */
    public ClientReliableTopicConfig(String name) {
        this.name = checkNotNull(name, "name");
    }

    /**
     * Create a clone of given reliable topic
     *
     * @param reliableTopicConfig topic
     */
    public ClientReliableTopicConfig(ClientReliableTopicConfig reliableTopicConfig) {
        this.executor = reliableTopicConfig.executor;
        this.readBatchSize = reliableTopicConfig.readBatchSize;
        this.name = reliableTopicConfig.name;
        this.topicOverloadPolicy = reliableTopicConfig.topicOverloadPolicy;
    }

    /**
     * Gets the name of the reliable topic.
     *
     * @return the name of the reliable topic.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name or name pattern for this config. Must not be modified after this
     * instance is added to {@link ClientConfig}.
     */
    public ClientReliableTopicConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the TopicOverloadPolicy for this reliable topic.
     *
     * @return the TopicOverloadPolicy.
     */
    public TopicOverloadPolicy getTopicOverloadPolicy() {
        return topicOverloadPolicy;
    }

    /**
     * Sets the TopicOverloadPolicy for this reliable topic. Check the {@link TopicOverloadPolicy} for more details about
     * this setting.
     *
     * @param topicOverloadPolicy the new TopicOverloadPolicy.
     * @return the updated reliable topic config.
     * @throws IllegalArgumentException if topicOverloadPolicy is null.
     */
    public ClientReliableTopicConfig setTopicOverloadPolicy(TopicOverloadPolicy topicOverloadPolicy) {
        this.topicOverloadPolicy = checkNotNull(topicOverloadPolicy, "topicOverloadPolicy can't be null");
        return this;
    }

    /**
     * Gets the Executor that is going to process the events.
     *
     * If no Executor is selected, then the {@link ExecutionService#ASYNC_EXECUTOR} is used.
     *
     * @return the Executor used to process events.
     * @see #setExecutor(Executor)
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Sets the Executor that is going to process the event.
     *
     * In some cases it is desirable to set a specific Executor. For example, you may want to isolate a certain topic from other
     * topics because it contains long running messages or very high priority messages.
     *
     * A single Executor can be shared between multiple Reliable topics, although it could take more time to process a message.
     * If a single Executor is not shared with other reliable topics, then the Executor only needs to have a single thread.
     *
     * @param executor the Executor. if the executor is null, the {@link ExecutionService#ASYNC_EXECUTOR} will
     *                 be used to process the event.
     * @return the updated config.
     */
    public ClientReliableTopicConfig setExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Gets the maximum number of items to read in a batch. Returned value will always be equal or larger than 1.
     *
     * @return the read batch size.
     */
    public int getReadBatchSize() {
        return readBatchSize;
    }

    /**
     * Sets the read batch size.
     *
     * The ReliableTopic tries to read a batch of messages from the ringbuffer. It will get at least one, but
     * if there are more available, then it will try to get more to increase throughput. The minimal read
     * batch size can be influenced using the read batch size.
     *
     * Apart from influencing the number of messages to download, the readBatchSize also determines how many
     * messages will be processed by the thread running the MessageListener before it returns back to the pool
     * to look for other MessageListeners that need to be processed. The problem with returning to the pool and
     * looking for new work is that interacting with an Executor is quite expensive due to contention on the
     * work-queue. The more work that can be done without retuning to the pool, the smaller the overhead.
     *
     * If the readBatchSize is 10 and there are 50 messages available, 10 items are retrieved and processed
     * consecutively before the thread goes back to the pool and helps out with the processing of other messages.
     *
     * If the readBatchSize is 10 and there are 2 items available, 2 items are retrieved and processed consecutively.
     *
     * If the readBatchSize is an issue because a thread will be busy too long with processing a single MessageListener
     * and it can't help out other MessageListeners, increase the size of the threadpool so the other MessageListeners don't
     * need to wait for a thread, but can be processed in parallel.
     *
     * @param readBatchSize the maximum number of items to read in a batch.
     * @return the updated reliable topic config.
     * @throws IllegalArgumentException if readBatchSize is smaller than 1.
     */
    public ClientReliableTopicConfig setReadBatchSize(int readBatchSize) {
        this.readBatchSize = checkPositive("readBatchSize", readBatchSize);
        return this;
    }

    @Override
    public String toString() {
        return "ClientReliableTopicConfig{"
                + "name='" + name + '\''
                + ", topicOverloadPolicy=" + topicOverloadPolicy
                + ", executor=" + executor
                + ", readBatchSize=" + readBatchSize
                + '}';
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientReliableTopicConfig that = (ClientReliableTopicConfig) o;

        if (readBatchSize != that.readBatchSize) {
            return false;
        }
        if (executor != null ? !executor.equals(that.executor) : that.executor != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return topicOverloadPolicy == that.topicOverloadPolicy;
    }

    @Override
    public int hashCode() {
        int result = executor != null ? executor.hashCode() : 0;
        result = 31 * result + readBatchSize;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (topicOverloadPolicy != null ? topicOverloadPolicy.hashCode() : 0);
        return result;
    }
}
