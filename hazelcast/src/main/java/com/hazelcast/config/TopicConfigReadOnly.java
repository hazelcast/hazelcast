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
import java.util.Collections;
import java.util.List;
/**
 * Configuration for topic(Read only)
 */

public class TopicConfigReadOnly extends TopicConfig {

    public TopicConfigReadOnly(TopicConfig config) {
        super(config);
    }

    public List<ListenerConfig> getMessageListenerConfigs() {
        final List<ListenerConfig> messageListenerConfigs = super.getMessageListenerConfigs();
        final List<ListenerConfig> readOnlyMessageListenerConfigs = new ArrayList<ListenerConfig>(messageListenerConfigs.size());
        for (ListenerConfig messageListenerConfig : messageListenerConfigs) {
            readOnlyMessageListenerConfigs.add(messageListenerConfig.getAsReadOnly());
        }
        return Collections.unmodifiableList(readOnlyMessageListenerConfigs);
    }

    public TopicConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only topic: " + getName());
    }

    public TopicConfig setGlobalOrderingEnabled(boolean globalOrderingEnabled) {
        throw new UnsupportedOperationException("This config is read-only topic: " + getName());
    }

    public TopicConfig addMessageListenerConfig(ListenerConfig listenerConfig) {
        throw new UnsupportedOperationException("This config is read-only topic: " + getName());
    }

    public TopicConfig setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config is read-only topic: " + getName());
    }

    public TopicConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only topic: " + getName());
    }
}
