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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Config for processing WAN events received from a target cluster.
 * You can configure certain behaviour when processing incoming WAN events
 * or even configure your own implementation for a WAN consumer. A custom
 * WAN consumer allows you to define custom processing logic and is usually
 * used in combination with a custom WAN publisher.
 * A custom consumer is optional and you may simply omit defining it which
 * will cause the default processing logic to be used.
 * <p>
 * NOTE: EE only
 *
 * @see WanReplicationConfig#setWanConsumerConfig(WanConsumerConfig)
 * @see WanPublisherConfig#setClassName(String)
 */
public class WanConsumerConfig implements IdentifiedDataSerializable {

    /**
     * @see #isPersistWanReplicatedData
     */
    public static final boolean DEFAULT_PERSIST_WAN_REPLICATED_DATA = false;

    private boolean persistWanReplicatedData = DEFAULT_PERSIST_WAN_REPLICATED_DATA;
    private String className;
    private Object implementation;
    private Map<String, Comparable> properties = new HashMap<String, Comparable>();

    /**
     * Returns the properties for the custom WAN consumer.
     */
    public Map<String, Comparable> getProperties() {
        return properties;
    }

    /**
     * Sets the properties for the custom WAN consumer. These properties are
     * accessible when initalizing the WAN consumer.
     *
     * @param properties the properties for the WAN consumer
     * @return this config
     */
    public WanConsumerConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the fully qualified class name of the class implementing
     * WanReplicationConsumer.
     *
     * @return fully qualified class name
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the fully qualified class name of the class implementing
     * WanReplicationConsumer.
     * The class name may be {@code null} in which case the implementation or
     * the default processing logic for incoming WAN events will be used.
     *
     * @param className fully qualified class name
     * @return this config
     * @see #setImplementation(Object)
     */
    public WanConsumerConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the implementation implementing WanReplicationConsumer.
     *
     * @return the implementation for this WAN consumer
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Sets the implementation for this WAN consumer. The object must implement
     * WanReplicationConsumer.
     * The implementation may be {@code null} in which case the class name or
     * the default processing logic for incoming WAN events will be used.
     *
     * @param implementation the object implementing WanReplicationConsumer
     * @return this config
     * @see #setClassName(String)
     */
    public WanConsumerConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * @return {@code true} when persistence of replicated data into backing
     * store is enabled, otherwise returns {@code false}. By default this
     * method returns {@value #DEFAULT_PERSIST_WAN_REPLICATED_DATA}.
     */
    public boolean isPersistWanReplicatedData() {
        return persistWanReplicatedData;
    }

    /**
     * @param persistWanReplicatedData set {@code true} to enable
     *                                 persistence of replicated data into backing store, otherwise set
     *                                 {@code false} to disable it. Default value is {@value
     *                                 #DEFAULT_PERSIST_WAN_REPLICATED_DATA}.
     * @return reference to this {@link WanReplicationRef} object
     */
    public WanConsumerConfig setPersistWanReplicatedData(boolean persistWanReplicatedData) {
        this.persistWanReplicatedData = persistWanReplicatedData;
        return this;
    }

    @Override
    public String toString() {
        return "WanConsumerConfig{"
                + "properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", persistWanReplicatedData=" + persistWanReplicatedData
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_CONSUMER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = properties.size();
        out.writeInt(size);
        for (Map.Entry<String, Comparable> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeUTF(className);
        out.writeObject(implementation);
        out.writeBoolean(persistWanReplicatedData);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), in.readObject());
        }
        className = in.readUTF();
        implementation = in.readObject();
        persistWanReplicatedData = in.readBoolean();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WanConsumerConfig that = (WanConsumerConfig) o;

        if (persistWanReplicatedData != that.persistWanReplicatedData) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = (persistWanReplicatedData ? 1 : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
