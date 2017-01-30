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

import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumType;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.quorum.QuorumType.READ_WRITE;

/**
 * Contains the configuration for cluster quorum.
 */
public class QuorumConfig {

    private String name;
    private boolean enabled;
    private int size;
    private List<QuorumListenerConfig> listenerConfigs = new ArrayList<QuorumListenerConfig>();
    private QuorumType type = READ_WRITE;
    private String quorumFunctionClassName;
    private QuorumFunction quorumFunctionImplementation;

    public QuorumConfig() {
    }

    public QuorumConfig(String name, boolean enabled) {
        this.name = name;
        this.enabled = enabled;
    }

    public QuorumConfig(String name, boolean enabled, int size) {
        this.name = name;
        this.enabled = enabled;
        this.size = size;
    }

    public QuorumConfig(QuorumConfig quorumConfig) {
        this.name = quorumConfig.name;
        this.enabled = quorumConfig.enabled;
        this.size = quorumConfig.size;
        this.listenerConfigs = quorumConfig.listenerConfigs;
        this.type = quorumConfig.type;
    }

    public String getName() {
        return name;
    }

    public QuorumConfig setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QuorumConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public int getSize() {
        return size;
    }

    public QuorumConfig setSize(int size) {
        if (size < 2) {
            throw new InvalidConfigurationException("Minimum quorum size cannot be less than 2");
        }
        this.size = size;
        return this;
    }

    public QuorumType getType() {
        return type;
    }

    public QuorumConfig setType(QuorumType type) {
        this.type = type;
        return this;
    }

    public List<QuorumListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }


    public QuorumConfig setListenerConfigs(List<QuorumListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
        return this;
    }

    public QuorumConfig addListenerConfig(QuorumListenerConfig listenerConfig) {
        this.listenerConfigs.add(listenerConfig);
        return this;
    }

    public String getQuorumFunctionClassName() {
        return quorumFunctionClassName;
    }

    public QuorumConfig setQuorumFunctionClassName(String quorumFunctionClassName) {
        this.quorumFunctionClassName = quorumFunctionClassName;
        return this;
    }

    public QuorumFunction getQuorumFunctionImplementation() {
        return quorumFunctionImplementation;
    }

    public QuorumConfig setQuorumFunctionImplementation(QuorumFunction quorumFunctionImplementation) {
        this.quorumFunctionImplementation = quorumFunctionImplementation;
        return this;
    }

    @Override
    public String toString() {
        return "QuorumConfig{"
                + "name='" + name + '\''
                + ", enabled=" + enabled
                + ", size=" + size
                + ", listenerConfigs=" + listenerConfigs
                + ", quorumFunctionClassName=" + quorumFunctionClassName
                + ", quorumFunctionImplementation=" + quorumFunctionImplementation
                + ", type=" + type + '}';
    }
}
