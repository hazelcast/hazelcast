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

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a {@link com.hazelcast.core.ITopic}.
 */
public class TopicConfig {

    /**
     * Default global ordering configuration.
     */
    public static final boolean DEFAULT_GLOBAL_ORDERING_ENABLED = false;

    private String name;
    private boolean globalOrderingEnabled = DEFAULT_GLOBAL_ORDERING_ENABLED;
    private boolean statisticsEnabled = true;
    private List<ListenerConfig> listenerConfigs;
    private TopicConfigReadOnly readOnly;

    /**
     * Creates a TopicConfig.
     */
    public TopicConfig() {
    }

    /**
     * Creates a TopicConfig with the given name.
     *
     * @param name the name of the Topic.
     */
    public TopicConfig(String name) {
        setName(name);
    }

    /**
     * Creates a {@link TopicConfig} by cloning another TopicConfig.
     *
     * @param config the TopicConfig to clone.
     */
    public TopicConfig(TopicConfig config) {
        isNotNull(config, "config");
        this.name = config.name;
        this.globalOrderingEnabled = config.globalOrderingEnabled;
        this.listenerConfigs = new ArrayList<ListenerConfig>(config.getMessageListenerConfigs());
    }

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
     * @param name the topic name to set.
     * @return the updated TopicConfig
     * @throws IllegalArgumentException if name is null or an empty string.
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
     * @param globalOrderingEnabled set to true to enable global ordering, false to disable
     * @return The updated TopicConfig
     */
    public TopicConfig setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
        this.globalOrderingEnabled = globalOrderingEnabled;
        return this;
    }

    /**
     * Adds a message listener to this topic (listens for when messages are added or removed).
     *
     * @param listenerConfig The message listener to add to this topic.
     */
    public TopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        getMessageListenerConfigs().add(listenerConfig);
        return this;
    }

    /**
     * Gets the list of message listeners (listens for when messages are added or removed) for this topic.
     *
     * @return The list of message listeners for this topic.
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
     * @param listenerConfigs The list of message listeners for this topic.
     * @return This updated topic configuration.
     */
    public TopicConfig setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    /**
     * Checks if statistics are enabled for this topic.
     *
     * @return true if statistics are enabled, false otherwise.
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics for this topic.
     *
     * @param statisticsEnabled True to enable statistics for this topic, false to disable.
     * @return the updated TopicConfig
     */
    public TopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public int hashCode() {
        return (globalOrderingEnabled ? 1231 : 1237)
                + 31 * (name != null ? name.hashCode() : 0);
    }

    /**
     * Checks if the given object is equal to this topic.
     *
     * @return true if the object is equal to this topic, false otherwise.
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TopicConfig)) {
            return false;
        }
        TopicConfig other = (TopicConfig) obj;
        return
                (this.name != null ? this.name.equals(other.name) : other.name == null)
                        && this.globalOrderingEnabled == other.globalOrderingEnabled;
    }

    public String toString() {
        return "TopicConfig [name=" + name + ", globalOrderingEnabled=" + globalOrderingEnabled + "]";
    }
}
