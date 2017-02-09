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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.nio.serialization.BinaryInterface.Reason.PUBLIC_API;

/**
 * Configuration object for WAN publishers.
 */
@BinaryInterface(reason = PUBLIC_API)
public class WanPublisherConfig implements DataSerializable {

    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final WANQueueFullBehavior DEFAULT_QUEUE_FULL_BEHAVIOR = WANQueueFullBehavior.DISCARD_AFTER_MUTATION;

    private String groupName = "dev";
    private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
    private WANQueueFullBehavior queueFullBehavior = DEFAULT_QUEUE_FULL_BEHAVIOR;
    private Map<String, Comparable> properties = new HashMap<String, Comparable>();
    private String className;
    private Object implementation;

    public String getGroupName() {
        return groupName;
    }

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

    @Override
    public String toString() {
        return "WanPublisherConfig{"
                + "properties=" + properties
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", groupName='" + groupName + '\''
                + ", queueCapacity=" + queueCapacity
                + ", queueFullBehavior=" + queueFullBehavior
                + '}';
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
