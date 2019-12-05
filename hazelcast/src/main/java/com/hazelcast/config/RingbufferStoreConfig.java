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
        this.factoryClassName = factoryClassName;
        return this;
    }

    public RingbufferStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public RingbufferStoreConfig setFactoryImplementation(RingbufferStoreFactory factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    public String toString() {
        return "RingbufferStoreConfig{"
                + "enabled=" + enabled
                + ", className='" + className + '\''
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
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RingbufferStoreConfig)) {
            return false;
        }

        RingbufferStoreConfig that = (RingbufferStoreConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (factoryClassName != null
                ? !factoryClassName.equals(that.factoryClassName) : that.factoryClassName != null) {
            return false;
        }
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
            return false;
        }
        if (storeImplementation != null
                ? !storeImplementation.equals(that.storeImplementation) : that.storeImplementation != null) {
            return false;
        }
        return factoryImplementation != null
                ? factoryImplementation.equals(that.factoryImplementation) : that.factoryImplementation == null;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (factoryClassName != null ? factoryClassName.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (storeImplementation != null ? storeImplementation.hashCode() : 0);
        result = 31 * result + (factoryImplementation != null ? factoryImplementation.hashCode() : 0);
        return result;
    }
}
