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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.TopicOverloadPolicy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.topic.TopicOverloadPolicy.BLOCK;

/**
 * Configuration for a reliable {@link ITopic}.
 * <p>
 * The reliable topic makes use of a {@link com.hazelcast.ringbuffer.Ringbuffer}
 * to store the actual messages.
 * <p>
 * To configure the ringbuffer for a reliable topic, define a ringbuffer in
 * the config with exactly the same name. It is very unlikely that you want
 * to run with the default settings.
 * <p>
 * When a ReliableTopic starts, it will always start from the {@code tail+1}
 * item from the RingBuffer. It will not chew its way through all available
 * events but it will wait for the next item being published.
 * <p>
 * In the reliable topic, global order is always maintained, so all listeners
 * will observe exactly the same order of sequence of messages.
 */
public class ReliableTopicConfig implements IdentifiedDataSerializable, NamedConfig {

    /**
     * The default read batch size.
     */
    public static final int DEFAULT_READ_BATCH_SIZE = 10;

    /**
     * The default slow consumer policy.
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

    public ReliableTopicConfig() {
    }

    /**
     * Creates a new ReliableTopicConfig with default settings.
     */
    public ReliableTopicConfig(String name) {
        this.name = checkNotNull(name, "name");
    }

    /**
     * Creates a new ReliableTopicConfig by cloning an existing one.
     *
     * @param config the ReliableTopicConfig to clone
     */
    public ReliableTopicConfig(ReliableTopicConfig config) {
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
     * Sets the name of  the reliable topic.
     *
     * @param name the name of the reliable topic
     * @return the updated ReliableTopicConfig
     * @throws IllegalArgumentException if name is {@code null} or an empty string
     */
    public ReliableTopicConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Gets the name of the reliable topic.
     *
     * @return the name of the reliable topic
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the TopicOverloadPolicy for this reliable topic.
     *
     * @return the TopicOverloadPolicy
     */
    public TopicOverloadPolicy getTopicOverloadPolicy() {
        return topicOverloadPolicy;
    }

    /**
     * Sets the TopicOverloadPolicy for this reliable topic. Check the
     * {@link TopicOverloadPolicy} for more details about this setting.
     *
     * @param topicOverloadPolicy the new TopicOverloadPolicy
     * @return the updated reliable topic config
     * @throws IllegalArgumentException if topicOverloadPolicy is {@code null}
     */
    public ReliableTopicConfig setTopicOverloadPolicy(TopicOverloadPolicy topicOverloadPolicy) {
        this.topicOverloadPolicy = checkNotNull(topicOverloadPolicy, "topicOverloadPolicy can't be null");
        return this;
    }

    /**
     * Gets the Executor that is going to process the events.
     * <p>
     * If no Executor is selected, then the
     * {@link ExecutionService#ASYNC_EXECUTOR} is used.
     *
     * @return the Executor used to process events
     * @see #setExecutor(java.util.concurrent.Executor)
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Sets the executor that is going to process the event.
     * <p>
     * In some cases it is desirable to set a specific executor. For example,
     * you may want to isolate a certain topic from other topics because it
     * contains long running messages or very high priority messages.
     * <p>
     * A single executor can be shared between multiple Reliable topics, although
     * it could take more time to process a message.
     * If a single executor is not shared with other reliable topics, then the
     * executor only needs to have a single thread.
     *
     * @param executor the executor. if the executor is null, the
     *                 {@link ExecutionService#ASYNC_EXECUTOR} will be used
     *                 to process the event
     * @return the updated config
     */
    public ReliableTopicConfig setExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Gets the maximum number of items to read in a batch. Returned value will
     * always be equal or larger than 1.
     *
     * @return the read batch size
     */
    public int getReadBatchSize() {
        return readBatchSize;
    }

    /**
     * Sets the read batch size.
     * <p>
     * The ReliableTopic tries to read a batch of messages from the ringbuffer.
     * It will get at least one, but if there are more available, then it will
     * try to get more to increase throughput. The maximum read batch size can
     * be influenced using the read batch size.
     * <p>
     * Apart from influencing the number of messages to retrieve, the
     * {@code readBatchSize} also determines how many messages will be processed
     * by the thread running the {@code MessageListener} before it returns back
     * to the pool to look for other {@code MessageListener}s that need to be
     * processed. The problem with returning to the pool and looking for new work
     * is that interacting with an executor is quite expensive due to contention
     * on the work-queue. The more work that can be done without retuning to the
     * pool, the smaller the overhead.
     * <p>
     * If the {@code readBatchSize} is 10 and there are 50 messages available,
     * 10 items are retrieved and processed consecutively before the thread goes
     * back to the pool and helps out with the processing of other messages.
     * <p>
     * If the {@code readBatchSize} is 10 and there are 2 items available,
     * 2 items are retrieved and processed consecutively.
     * <p>
     * If the {@code readBatchSize} is an issue because a thread will be busy
     * too long with processing a single {@code MessageListener} and it can't
     * help out other {@code MessageListener}s, increase the size of the
     * threadpool so the other {@code MessageListener}s don't need to wait for
     * a thread, but can be processed in parallel.
     *
     * @param readBatchSize the maximum number of items to read in a batch
     * @return the updated reliable topic config
     * @throws IllegalArgumentException if the {@code readBatchSize} is smaller than 1
     */
    public ReliableTopicConfig setReadBatchSize(int readBatchSize) {
        this.readBatchSize = checkPositive("readBatchSize", readBatchSize);
        return this;
    }

    /**
     * Checks if statistics are enabled for this reliable topic.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this reliable topic.
     * Collects the creation time, total number of published and received
     * messages for each member locally.
     *
     * @param statisticsEnabled {@code true} to enable statistics, {@code false} to disable
     * @return the updated reliable topic config
     */
    public ReliableTopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    /**
     * Sets the list of message listeners (listens for when messages are added
     * or removed) for this topic.
     *
     * @param listenerConfigs the list of message listeners for this topic
     * @return this updated topic configuration
     */
    public ReliableTopicConfig setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs != null ? listenerConfigs : new LinkedList<ListenerConfig>();
        return this;
    }

    /**
     * Gets the list of message listeners (listens for when messages are added
     * or removed) for this reliable topic.
     *
     * @return list of MessageListener configurations
     */
    public List<ListenerConfig> getMessageListenerConfigs() {
        return listenerConfigs;
    }

    /**
     * Adds a message listener (listens for when messages are added or removed)
     * to this reliable topic.
     *
     * @param listenerConfig the ListenerConfig to add
     * @return the updated config
     * @throws NullPointerException if listenerConfig is {@code null}
     */
    public ReliableTopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        checkNotNull(listenerConfig, "listenerConfig can't be null");
        listenerConfigs.add(listenerConfig);
        return this;
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

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.RELIABLE_TOPIC_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(executor);
        out.writeInt(readBatchSize);
        out.writeString(name);
        out.writeBoolean(statisticsEnabled);
        writeNullableList(listenerConfigs, out);
        out.writeString(topicOverloadPolicy.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        executor = in.readObject();
        readBatchSize = in.readInt();
        name = in.readString();
        statisticsEnabled = in.readBoolean();
        listenerConfigs = readNullableList(in);
        topicOverloadPolicy = TopicOverloadPolicy.valueOf(in.readString());
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReliableTopicConfig)) {
            return false;
        }

        ReliableTopicConfig that = (ReliableTopicConfig) o;

        if (readBatchSize != that.readBatchSize) {
            return false;
        }
        if (statisticsEnabled != that.statisticsEnabled) {
            return false;
        }
        if (executor != null ? !executor.equals(that.executor) : that.executor != null) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (listenerConfigs != null ? !listenerConfigs.equals(that.listenerConfigs) : that.listenerConfigs != null) {
            return false;
        }
        return topicOverloadPolicy == that.topicOverloadPolicy;
    }

    @Override
    public final int hashCode() {
        int result = executor != null ? executor.hashCode() : 0;
        result = 31 * result + readBatchSize;
        result = 31 * result + name.hashCode();
        result = 31 * result + (statisticsEnabled ? 1 : 0);
        result = 31 * result + (listenerConfigs != null ? listenerConfigs.hashCode() : 0);
        result = 31 * result + (topicOverloadPolicy != null ? topicOverloadPolicy.hashCode() : 0);
        return result;
    }
}
