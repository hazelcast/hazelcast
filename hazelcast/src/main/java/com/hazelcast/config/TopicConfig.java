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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class TopicConfig implements DataSerializable {

    public final static boolean DEFAULT_GLOBAL_ORDERING_ENABLED = true;
    private String name;
    private boolean globalOrderingEnabled = DEFAULT_GLOBAL_ORDERING_ENABLED;
    private boolean statisticsEnabled = true;
    private List<ListenerConfig> listenerConfigs;

    public TopicConfig() {
    }

    public TopicConfig(TopicConfig config) {
        this.name = config.name;
        this.globalOrderingEnabled = config.globalOrderingEnabled;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public TopicConfig setName(String name) {
        this.name = name;
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

    public void setMessageListenerConfigs(List<ListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public int hashCode() {
        return (globalOrderingEnabled ? 1231 : 1237) +
                31 * (name != null ? name.hashCode() : 0);
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof TopicConfig))
            return false;
        TopicConfig other = (TopicConfig) obj;
        return
                (this.name != null ? this.name.equals(other.name) : other.name == null) &&
                        this.globalOrderingEnabled == other.globalOrderingEnabled;
    }

    public String toString() {
        return "TopicConfig [name=" + name + ", globalOrderingEnabled=" + globalOrderingEnabled + "]";
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(globalOrderingEnabled);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        globalOrderingEnabled = in.readBoolean();
    }
}
