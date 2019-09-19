/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.internal.util.MapUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This configuration class describes a {@link com.hazelcast.spi.discovery.DiscoveryStrategy}
 * based on a parsed XML or configured manually using the config API
 */
public class DiscoveryStrategyConfig implements IdentifiedDataSerializable {
    private String className;
    // we skip serialization since this may be a user-supplied object and
    // it may not be serializable. Since we send the WAN config in the
    // FinalizeJoinOp, this may prevent a node from sending it to a joining
    // member
    private transient DiscoveryStrategyFactory discoveryStrategyFactory;
    private Map<String, Comparable> properties;

    public DiscoveryStrategyConfig() {
        properties = MapUtil.createHashMap(1);
    }

    public DiscoveryStrategyConfig(String className) {
        this(className, Collections.<String, Comparable>emptyMap());
    }

    public DiscoveryStrategyConfig(String className,
                                   Map<String, Comparable> properties) {
        this.className = className;
        this.properties = properties == null
                ? MapUtil.<String, Comparable>createHashMap(1)
                : new HashMap<String, Comparable>(properties);
        this.discoveryStrategyFactory = null;
    }


    public DiscoveryStrategyConfig(DiscoveryStrategyFactory discoveryStrategyFactory) {
        this(discoveryStrategyFactory, Collections.<String, Comparable>emptyMap());
    }

    public DiscoveryStrategyConfig(DiscoveryStrategyFactory discoveryStrategyFactory, Map<String, Comparable> properties) {
        this.className = null;
        this.properties = properties == null
                ? MapUtil.<String, Comparable>createHashMap(1)
                : new HashMap<String, Comparable>(properties);
        this.discoveryStrategyFactory = discoveryStrategyFactory;
    }

    public DiscoveryStrategyConfig(DiscoveryStrategyConfig config) {
        className = config.className;
        discoveryStrategyFactory = config.discoveryStrategyFactory;
        properties = new HashMap<String, Comparable>(config.properties);
    }

    public String getClassName() {
        return className;
    }

    public DiscoveryStrategyConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public DiscoveryStrategyConfig setDiscoveryStrategyFactory(DiscoveryStrategyFactory discoveryStrategyFactory) {
        this.discoveryStrategyFactory = discoveryStrategyFactory;
        return this;
    }

    public DiscoveryStrategyFactory getDiscoveryStrategyFactory() {
        return discoveryStrategyFactory;
    }

    public void addProperty(String key, Comparable value) {
        properties.put(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public DiscoveryStrategyConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties == null
                ? MapUtil.<String, Comparable>createHashMap(1)
                : new HashMap<String, Comparable>(properties);
        return this;
    }

    public Map<String, Comparable> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "DiscoveryStrategyConfig{"
                + "properties=" + properties
                + ", className='" + className + '\''
                + ", discoveryStrategyFactory=" + discoveryStrategyFactory
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DISCOVERY_STRATEGY_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);

        out.writeInt(properties.size());
        for (Map.Entry<String, Comparable> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), (Comparable) in.readObject());
        }
    }
}
