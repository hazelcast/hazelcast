/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.ValidationUtil.hasText;
import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the configuration for a {@link com.hazelcast.core.ITopic}.
 */
public class TopicConfig {

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
     * Creates a  {@link TopicConfig} by cloning another TopicConfig.
     *
     * @param config
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
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the topic.
     *
     * @param name the name to set
     * @return the updated TopicConfig
     * @throws IllegalArgumentException if name is null or an empty string.
     */
    public TopicConfig setName(String name) {
        this.name = hasText(name, "name");
        return this;
    }

    /**
     * @return the globalOrderingEnabled
     */
    public boolean isGlobalOrderingEnabled() {
        return globalOrderingEnabled;
    }

    /**
     * @param globalOrderingEnabled the globalOrderingEnabled to set
     */
    public TopicConfig setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
        this.globalOrderingEnabled = globalOrderingEnabled;
        return this;
    }

    public TopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        getMessageListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ListenerConfig> getMessageListenerConfigs() {
        if (listenerConfigs == null) {
            listenerConfigs = new ArrayList<ListenerConfig>();
        }
        return listenerConfigs;
    }

    public TopicConfig setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
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
     * @param statisticsEnabled
     * @return
     */
    public TopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public int hashCode() {
        return (globalOrderingEnabled ? 1231 : 1237)
                + 31 * (name != null ? name.hashCode() : 0);
    }

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
