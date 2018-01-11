/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Configuration object for WAN publishers. A single publisher defines how WAN events are sent to a specific endpoint.
 * The endpoint can be a different cluster defined by static IP's or discovered using a cloud discovery mechanism.
 *
 * @see DiscoveryConfig
 * @see AwsConfig
 */
public class WanPublisherConfig implements IdentifiedDataSerializable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final WANQueueFullBehavior DEFAULT_QUEUE_FULL_BEHAVIOR = WANQueueFullBehavior.DISCARD_AFTER_MUTATION;

    private String groupName = "dev";
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private WANQueueFullBehavior queueFullBehavior = DEFAULT_QUEUE_FULL_BEHAVIOR;
    private Map<String, Comparable> properties = new HashMap<String, Comparable>();
    private String className;
    private Object implementation;
    private AwsConfig awsConfig = new AwsConfig();
    private DiscoveryConfig discoveryConfig = new DiscoveryConfig();

    /**
     * Return the group name of this publisher. The group name is used for identifying the publisher in a
     * {@link WanReplicationConfig} and for authentication on the target endpoint.
     *
     * @return the publisher group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Set the group name of this publisher. The group name is used for identifying the publisher in a
     * {@link WanReplicationConfig} and for authentication on the target endpoint.
     *
     * @param groupName the publisher group name
     * @return this config
     */
    public WanPublisherConfig setGroupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    /**
     * Get the capacity of the queue for WAN replication events. IMap, ICache, normal and backup events count against
     * the queue capacity separately. When the queue capacity is reached, backup events are dropped while normal
     * replication events behave as determined by the {@link #getQueueFullBehavior()}.
     * The default queue size for replication queues is {@value #DEFAULT_QUEUE_CAPACITY}.
     *
     * @return the queue capacity
     */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * Set the capacity of the queue for WAN replication events. IMap, ICache, normal and backup events count against
     * the queue capacity separately. When the queue capacity is reached, backup events are dropped while normal
     * replication events behave as determined by the {@link #getQueueFullBehavior()}.
     * The default queue size for replication queues is {@value #DEFAULT_QUEUE_CAPACITY}.
     *
     * @param queueCapacity the queue capacity
     * @return this configuration
     */
    public WanPublisherConfig setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        return this;
    }

    public WANQueueFullBehavior getQueueFullBehavior() {
        return queueFullBehavior;
    }

    public WanPublisherConfig setQueueFullBehavior(WANQueueFullBehavior queueFullBehavior) {
        this.queueFullBehavior = queueFullBehavior;
        return this;
    }

    public Map<String, Comparable> getProperties() {
        return properties;
    }

    public WanPublisherConfig setProperties(Map<String, Comparable> properties) {
        this.properties = properties;
        return this;
    }

    public String getClassName() {
        return className;
    }

    /**
     * Set the name of the class implementing the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this class should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     *
     * @param className the name of the class implementation for the WAN replication
     * @return the wan publisher config
     */
    public WanPublisherConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Object getImplementation() {
        return implementation;
    }

    /**
     * Set the implementation of the WanReplicationEndpoint.
     * NOTE: OS and EE have different interfaces that this object should implement.
     * For OS see {@link com.hazelcast.wan.WanReplicationEndpoint}.
     *
     * @param implementation the implementation for the WAN replication
     * @return the wan publisher config
     */
    public WanPublisherConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * @return the awsConfig join configuration
     */
    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    /**
     * @param awsConfig the AwsConfig join configuration to set
     * @throws IllegalArgumentException if awsConfig is null
     */
    public WanPublisherConfig setAwsConfig(final AwsConfig awsConfig) {
        this.awsConfig = isNotNull(awsConfig, "awsConfig");
        return this;
    }

    /**
     * Returns the currently defined {@link DiscoveryConfig}
     *
     * @return current DiscoveryProvidersConfig instance
     */
    public DiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    /**
     * Sets a custom defined {@link DiscoveryConfig}
     *
     * @param discoveryConfig configuration to set
     * @throws java.lang.IllegalArgumentException if discoveryProvidersConfig is null
     */
    public WanPublisherConfig setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = isNotNull(discoveryConfig, "discoveryProvidersConfig");
        return this;
    }

    @Override
    public String toString() {
        return "WanPublisherConfig{"
                + "groupName='" + groupName + '\''
                + ", queueCapacity=" + queueCapacity
                + ", queueFullBehavior=" + queueFullBehavior
                + ", properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", awsConfig=" + awsConfig
                + ", discoveryConfig=" + discoveryConfig
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.WAN_PUBLISHER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeInt(queueCapacity);
        out.writeInt(queueFullBehavior.getId());
        int size = properties.size();
        out.writeInt(size);
        for (Map.Entry<String, Comparable> entry : properties.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
        out.writeUTF(className);
        out.writeObject(implementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        queueCapacity = in.readInt();
        queueFullBehavior =  WANQueueFullBehavior.getByType(in.readInt());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), (Comparable) in.readObject());
        }
        className = in.readUTF();
        implementation = in.readObject();
    }
}
