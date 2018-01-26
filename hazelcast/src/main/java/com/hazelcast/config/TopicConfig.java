/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a {@link com.hazelcast.core.ITopic}.
 */
public class TopicConfig implements IdentifiedDataSerializable {

    /**
     * Default global ordering configuration.
     */
    public static final boolean DEFAULT_GLOBAL_ORDERING_ENABLED = false;

    private String name;
    private boolean globalOrderingEnabled = DEFAULT_GLOBAL_ORDERING_ENABLED;
    private boolean statisticsEnabled = true;
    private boolean multiThreadingEnabled;
    private List<ListenerConfig> listenerConfigs;
    private transient TopicConfigReadOnly readOnly;

    /**
     * Creates a TopicConfig.
     */
    public TopicConfig() {
    }

    /**
     * Creates a TopicConfig with the given name.
     *
     * @param name the name of the Topic
     */
    public TopicConfig(String name) {
        setName(name);
    }

    /**
     * Creates a {@link TopicConfig} by cloning another TopicConfig.
     *
     * @param config the TopicConfig to clone
     */
    public TopicConfig(TopicConfig config) {
        isNotNull(config, "config");
        this.name = config.name;
        this.globalOrderingEnabled = config.globalOrderingEnabled;
        this.multiThreadingEnabled = config.multiThreadingEnabled;
        this.listenerConfigs = new ArrayList<ListenerConfig>(config.getMessageListenerConfigs());
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public TopicConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new TopicConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the topic, null if nothing is set.
     *
     * @return the name of the topic
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the topic.
     *
     * @param name the topic name to set
     * @return the updated {@link TopicConfig}
     * @throws IllegalArgumentException if name is {@code null} or an empty string
     */
    public TopicConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Checks if global ordering is enabled (all nodes listening to the same topic
     * get their messages in the same order), or disabled (nodes get
     * the messages in the order that the messages are published).
     *
     * @return true if global ordering is enabled, false if disabled
     */
    public boolean isGlobalOrderingEnabled() {
        return globalOrderingEnabled;
    }

    /**
     * Enable global ordering (all nodes listening to the same topic
     * get their messages in the same order), or disable it (nodes get
     * the messages in the order that the messages are published).
     *
     * @param globalOrderingEnabled set to {@code true} to enable global ordering, {@code false} to disable
     * @return the updated TopicConfig
     */
    public TopicConfig setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
        if (this.multiThreadingEnabled && globalOrderingEnabled) {
            throw new IllegalArgumentException("Global ordering can not be enabled when multi-threading is used.");
        }
        this.globalOrderingEnabled = globalOrderingEnabled;
        return this;
    }

    /**
     * Checks if multi-threaded processing of incoming messages is enabled or not.
     * When disabled only one dedicated thread will handle all topic messages. Otherwise
     * any thread from events thread pool can be used for message handling.
     *
     * @return {@code true} if multi-threading is enabled, {@code false} if disabled
     */
    public boolean isMultiThreadingEnabled() {
        return multiThreadingEnabled;
    }


    /**
     * Enable multi-threaded message handling. When enabled any thread from events
     * thread pool can be used for incoming message processing. Otherwise only one
     * dedicated thread will be used to handle topic messages.
     * Note: it can be enabled only in case when global ordering is disabled. Moreover,
     * the local message ordering is not supported in this mode also. This means the
     * messages produced by local publisher can be processed by several threads with
     * no ordering guarantee.
     *
     * @param multiThreadingEnabled set to {@code true} to enable multi-threaded message processing, {@code false} to disable
     * @return the updated TopicConfig
     */
    public TopicConfig setMultiThreadingEnabled(boolean multiThreadingEnabled) {
        if (this.globalOrderingEnabled && multiThreadingEnabled) {
            throw new IllegalArgumentException("Multi-threading can not be enabled when global ordering is used.");
        }
        this.multiThreadingEnabled = multiThreadingEnabled;
        return this;
    }

    /**
     * Adds a message listener to this topic (listens for when messages are added or removed).
     *
     * @param listenerConfig the message listener to add to this topic
     */
    public TopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        getMessageListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Gets the list of message listeners (listens for when messages are added or removed) for this topic.
     *
     * @return the list of message listeners for this topic
     */
    public List<ListenerConfig> getMessageListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ListenerConfig>();
        }
        return listenerConfigs;
    }

    /**
     * Sets the list of message listeners (listens for when messages are added or removed) for this topic.
     *
     * @param listenerConfigs the list of message listeners for this topic
     * @return this updated topic configuration
     */
    public TopicConfig setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    /**
     * Checks if statistics are enabled for this topic.
     *
     * @return {@code true} if statistics are enabled, {@code false} otherwise
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this topic.
     *
     * @param statisticsEnabled {@code true} to enable statistics for this topic, {@code false} to disable
     * @return the updated TopicConfig
     */
    public TopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public int hashCode() {
        return 31 * (name != null ? name.hashCode() : 0);
    }

    /**
     * Checks if the given object is equal to this topic.
     *
     * @return {@code true} if the object is equal to this topic, {@code false} otherwise
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TopicConfig)) {
            return false;
        }
        TopicConfig other = (TopicConfig) obj;
        return (this.name != null ? this.name.equals(other.name) : other.name == null);
    }

    public String toString() {
        return "TopicConfig [name=" + name + ", globalOrderingEnabled=" + globalOrderingEnabled
                + ", multiThreadingEnabled=" + multiThreadingEnabled + ", statisticsEnabled="
                + statisticsEnabled + "]";
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.TOPIC_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(globalOrderingEnabled);
        out.writeBoolean(statisticsEnabled);
        out.writeBoolean(multiThreadingEnabled);
        writeNullableList(listenerConfigs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        globalOrderingEnabled = in.readBoolean();
        statisticsEnabled = in.readBoolean();
        multiThreadingEnabled = in.readBoolean();
        listenerConfigs = readNullableList(in);
    }
}
