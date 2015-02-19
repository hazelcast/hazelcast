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

package com.hazelcast.config;

import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import static com.hazelcast.topic.TopicOverloadPolicy.BLOCK;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for the a reliable {@link com.hazelcast.core.ITopic}.
 *
 * The reliable topic makes use of the {@link com.hazelcast.ringbuffer.Ringbuffer} to store the actual messages.
 *
 * To configure the ringbuffer for a reliable topic, define a ringbuffer in the config with exactly the same name. It is very
 * unlikely that you want to run with the default settings.
 *
 * When a ReliableTopic start, it will always start from the tail+1 item from the RingBuffer. It will not chew its way through
 * all available events but it will wait for the next item being published.
 *
 * In the reliable topic global order is always maintained. So all listeners will observe exactly the same order of sequence of
 * messages.
 */
@Beta
public class ReliableTopicConfig {

    /**
     * The default read batch size.
     */
    public static final int DEFAULT_READ_BATCH_SIZE = 10;

    /**
     * The default slow consumer policy
     */
    public static final TopicOverloadPolicy DEFAULT_TOPIC_OVERLOAD_POLICY = BLOCK;

    /**
     * Default value for statistics enabled.
     */
    public static final boolean DEFAULT_STATISTICS_ENABLED = true;

    private Executor executor;
    private int readBatchSize = DEFAULT_READ_BATCH_SIZE;
    private String name;
    private boolean statisticsEnabled = DEFAULT_STATISTICS_ENABLED;
    private List<ListenerConfig> listenerConfigs = new LinkedList<ListenerConfig>();
    private TopicOverloadPolicy topicOverloadPolicy = DEFAULT_TOPIC_OVERLOAD_POLICY;

    /**
     * Creates a new ReliableTopicConfig with default settings.
     */
    public ReliableTopicConfig(String name) {
        this.name = checkNotNull(name, "name");
    }

    /**
     * Creates a new ReliableTopicConfig by cloning an existing one.
     *
     * @param config the ReliableTopicConfig to clone.
     */
    ReliableTopicConfig(ReliableTopicConfig config) {
        this.name = config.name;
        this.statisticsEnabled = config.statisticsEnabled;
        this.readBatchSize = config.readBatchSize;
        this.executor = config.executor;
        this.topicOverloadPolicy = config.topicOverloadPolicy;
        this.listenerConfigs = config.listenerConfigs;
    }

    ReliableTopicConfig(ReliableTopicConfig config, String name) {
        this(config);
        this.name = name;
    }

    /**
     * Gets the name of the topic.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the TopicOverloadPolicy
     *
     * @return the TopicOverloadPolicy.
     */
    public TopicOverloadPolicy getTopicOverloadPolicy() {
        return topicOverloadPolicy;
    }

    /**
     * Sets the TopicOverloadPolicy. Check the {@link TopicOverloadPolicy} for more details about this setting.
     *
     * @param topicOverloadPolicy the new TopicOverloadPolicy.
     * @return the updated config.
     * @throws IllegalArgumentException if topicOverloadPolicy is null.
     */
    public ReliableTopicConfig setTopicOverloadPolicy(TopicOverloadPolicy topicOverloadPolicy) {
        this.topicOverloadPolicy = checkNotNull(topicOverloadPolicy, "topicOverloadPolicy can't be null");
        return this;
    }

    /**
     * Gets the Executor that is going to process the events.
     *
     * If no Executor is selected, then the {@link com.hazelcast.spi.ExecutionService#ASYNC_EXECUTOR} is used.
     *
     * @return the Executor used to process events.
     * @see #setExecutor(java.util.concurrent.Executor)
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Sets the Executor that is going to process the event.
     *
     * In some cases it is desirable to set a specific Executor. For example you want to isolate a certain topic from other
     * topic e.g. because it contains long running messages or very high priority messages.
     *
     * A single Executor can be shared between multiple Reliable topics, although it could take more time to process a message.
     * If a single Executor is not shared with other reliable topics, then the Executor only needs to have a single thread.
     *
     * @param executor the Executor. if the executor is null, the {@link com.hazelcast.spi.ExecutionService#ASYNC_EXECUTOR} will
     *                 be used to process the event.
     * @return the updated config.
     */
    public ReliableTopicConfig setExecutor(Executor executor) {
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
     * work-queue. So the more work that can be done without retuning to the pool, the smaller the overhead.
     *
     * So if the readBatchSize is 10 and there are 50 messages available, 10 items are retrieved and processed
     * consecutively before the thread is going back to the pool and help out with the processing of other messages.
     *
     * If the readBatchSize is 10 and there are 2 items available, 2 items are retrieved and processed consecutively.
     *
     * If the readBatchSize is an issue because a thread will be busy too long with processing a single MessageListener
     * and can't help out other MessageListener, increase the size of the threadpool so the other MessageListener don't
     * need to wait for a thread, but can be processed in parallel.
     *
     * @param readBatchSize the readBatchSize
     * @return the updated config.
     * @throws IllegalArgumentException if readBatchSize is smaller than 1.
     */
    public ReliableTopicConfig setReadBatchSize(int readBatchSize) {
        this.readBatchSize = checkPositive(readBatchSize, "readBatchSize should be positive");
        return this;
    }

    /**
     * Checks if statistics are enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enabled or disabled statistics.
     *
     * @param statisticsEnabled if statistics should be enabled
     * @return the updated config.
     */
    public ReliableTopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Gets the List of MessageListener configurations.
     *
     * @return list of MessageListener configuration.
     */
    public List<ListenerConfig> getMessageListenerConfigs() {
        return listenerConfigs;
    }

    /**
     * Adds a ListenerConfig.
     *
     * @param listenerConfig the ListenerConfig to add.
     * @return the updated config.
     * @throws NullPointerException if listenerConfig is null.
     */
    public ReliableTopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        checkNotNull(listenerConfig, "listenerConfig can't be null");
        listenerConfigs.add(listenerConfig);
        return this;
    }

    /**
     * Returns a readonly version of the ReliableTopicConfig.
     *
     * @return a readonly version.
     */
    public ReliableTopicConfig getAsReadOnly() {
        return new ReliableTopicConfigReadOnly(this);
    }

    @Override
    public String toString() {
        return "ReliableTopicConfig{"
                + "name='" + name + '\''
                + ", topicOverloadPolicy=" + topicOverloadPolicy
                + ", executor=" + executor
                + ", readBatchSize=" + readBatchSize
                + ", statisticsEnabled=" + statisticsEnabled
                + ", listenerConfigs=" + listenerConfigs
                + '}';
    }

    static class ReliableTopicConfigReadOnly extends ReliableTopicConfig {

        public ReliableTopicConfigReadOnly(ReliableTopicConfig config) {
            super(config);
        }

        @Override
        public ReliableTopicConfig setExecutor(Executor executor) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ReliableTopicConfig setReadBatchSize(int readBatchSize) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ReliableTopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ReliableTopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public ReliableTopicConfig setTopicOverloadPolicy(TopicOverloadPolicy topicOverloadPolicy) {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
