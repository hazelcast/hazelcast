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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Base class for WAN publisher configuration.
 */
public abstract class AbstractWanPublisherConfig implements IdentifiedDataSerializable {

    protected String publisherId = "";
    protected String className = "";
    protected Object implementation;
    protected Map<String, Comparable> properties = new HashMap<>();

    /**
     * Returns the publisher ID used for identifying the publisher in a
     * {@link WanReplicationConfig}.
     *
     * @return the WAN publisher ID or {@code null} if no publisher ID is set
     */
    public String getPublisherId() {
        return publisherId;
    }

    /**
     * Sets the publisher ID used for identifying the publisher in a
     * {@link WanReplicationConfig}.
     *
     * @param publisherId the WAN publisher ID
     * @return this config
     */
    public AbstractWanPublisherConfig setPublisherId(String publisherId) {
        this.publisherId = publisherId;
        return this;
    }

    /**
     * Returns the WAN publisher properties.
     */
    public @Nonnull
    Map<String, Comparable> getProperties() {
        return properties;
    }

    /**
     * Sets the WAN publisher properties.
     *
     * @param properties WAN publisher properties
     * @return this config
     */
    public AbstractWanPublisherConfig setProperties(@Nonnull Map<String, Comparable> properties) {
        this.properties = checkNotNull(properties, "Properties must not be null");
        return this;
    }

    /**
     * Returns the name of the class implementing
     * {@link com.hazelcast.wan.WanReplicationPublisher}.
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name of the class implementing
     * {@link com.hazelcast.wan.WanReplicationPublisher}.
     * To configure the built in WanBatchReplication, please use
     * {@link WanBatchReplicationPublisherConfig} config class.
     *
     * @param className the name of the class implementation for the WAN replication
     * @return this config
     */
    public AbstractWanPublisherConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * Returns the implementation of {@link com.hazelcast.wan.WanReplicationPublisher}.
     */
    public Object getImplementation() {
        return implementation;
    }

    /**
     * Sets the implementation of {@link com.hazelcast.wan.WanReplicationPublisher}.
     *
     * @param implementation the implementation for the WAN replication
     * @return this config
     */
    public AbstractWanPublisherConfig setImplementation(Object implementation) {
        this.implementation = implementation;
        return this;
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
        out.writeUTF(publisherId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            properties.put(in.readUTF(), in.readObject());
        }
        className = in.readUTF();
        implementation = in.readObject();
        publisherId = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractWanPublisherConfig that = (AbstractWanPublisherConfig) o;

        if (!publisherId.equals(that.publisherId)) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        int result = publisherId.hashCode();
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + properties.hashCode();
        return result;
    }
}
