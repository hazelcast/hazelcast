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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Configuration for the {@link RingbufferStore}.
 */
public class RingbufferStoreConfig implements IdentifiedDataSerializable {

    private boolean enabled = true;
    private String className;
    private String factoryClassName;
    private Properties properties = new Properties();
    private RingbufferStore storeImplementation;
    private RingbufferStoreFactory factoryImplementation;

    public RingbufferStoreConfig() {
    }

    public RingbufferStoreConfig(RingbufferStoreConfig config) {
        enabled = config.isEnabled();
        className = config.getClassName();
        storeImplementation = config.getStoreImplementation();
        factoryClassName = config.getFactoryClassName();
        factoryImplementation = config.getFactoryImplementation();
        properties.putAll(config.getProperties());
    }

    public RingbufferStore getStoreImplementation() {
        return storeImplementation;
    }

    public RingbufferStoreConfig setStoreImplementation(RingbufferStore storeImplementation) {
        if (storeImplementation != null) {
            setClassName(null);
        }
        this.storeImplementation = storeImplementation;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RingbufferStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public RingbufferStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public RingbufferStoreConfig setProperties(Properties properties) {
        this.properties = isNotNull(properties, "properties");
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public RingbufferStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public RingbufferStoreConfig setFactoryClassName(String factoryClassName) {
        if (factoryClassName != null) {
            setFactoryImplementation(null);
        }
        this.factoryClassName = factoryClassName;
        return this;
    }

    public RingbufferStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public RingbufferStoreConfig setFactoryImplementation(RingbufferStoreFactory factoryImplementation) {
        if (factoryImplementation != null) {
            setFactoryClassName(null);
        }
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    public String toString() {
        return "RingbufferStoreConfig{"
                + "enabled=" + enabled
                + ", className='" + storeImplementationNameInternal() + '\''
                + ", factoryClassName='" + storeFactoryImplementationNameInternal() + '\''
                + ", properties=" + properties
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.RINGBUFFER_STORE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeUTF(className);
        out.writeUTF(factoryClassName);
        out.writeObject(properties);
        out.writeObject(storeImplementation);
        out.writeObject(factoryImplementation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        className = in.readUTF();
        factoryClassName = in.readUTF();
        properties = in.readObject();
        storeImplementation = in.readObject();
        factoryImplementation = in.readObject();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RingbufferStoreConfig)) {
            return false;
        }

        RingbufferStoreConfig that = (RingbufferStoreConfig) o;

        return enabled == that.enabled
            && Objects.equals(storeImplementationNameInternal(), that.storeImplementationNameInternal())
            && Objects.equals(properties, that.properties)
            && Objects.equals(storeFactoryImplementationNameInternal(), that.storeFactoryImplementationNameInternal());
    }

    private String storeImplementationNameInternal() {
        if (storeImplementation != null) {
            return storeImplementation.getClass().getName();
        }
        return className;
    }

    private String storeFactoryImplementationNameInternal() {
        if (factoryImplementation != null) {
            return factoryImplementation.getClass().getName();
        }
        return factoryClassName;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(enabled, properties, storeImplementationNameInternal(), storeFactoryImplementationNameInternal());
    }
}
