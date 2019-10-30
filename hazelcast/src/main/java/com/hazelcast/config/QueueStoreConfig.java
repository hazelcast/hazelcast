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

import com.hazelcast.collection.QueueStore;
import com.hazelcast.collection.QueueStoreFactory;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Configuration for the {@link QueueStore}.
 */
public class QueueStoreConfig implements IdentifiedDataSerializable {

    private boolean enabled = true;
    private String className;
    private String factoryClassName;
    private Properties properties = new Properties();
    private QueueStore storeImplementation;
    private QueueStoreFactory factoryImplementation;

    public QueueStoreConfig() {
    }

    public QueueStoreConfig(QueueStoreConfig config) {
        enabled = config.isEnabled();
        className = config.getClassName();
        storeImplementation = config.getStoreImplementation();
        factoryClassName = config.getFactoryClassName();
        factoryImplementation = config.getFactoryImplementation();
        properties.putAll(config.getProperties());
    }

    public QueueStore getStoreImplementation() {
        return storeImplementation;
    }

    public QueueStoreConfig setStoreImplementation(QueueStore storeImplementation) {
        this.storeImplementation = storeImplementation;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public QueueStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public QueueStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public QueueStoreConfig setProperties(Properties properties) {
        this.properties = isNotNull(properties, "properties");
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public QueueStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public QueueStoreConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    public QueueStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public QueueStoreConfig setFactoryImplementation(QueueStoreFactory factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    public String toString() {
        return "QueueStoreConfig{"
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
        return ConfigDataSerializerHook.QUEUE_STORE_CONFIG;
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
        if (!(o instanceof QueueStoreConfig)) {
            return false;
        }

        QueueStoreConfig that = (QueueStoreConfig) o;
        if (isEnabled() != that.isEnabled()) {
            return false;
        }
        if (getClassName() != null ? !getClassName().equals(that.getClassName()) : that.getClassName() != null) {
            return false;
        }
        if (getFactoryClassName() != null ? !getFactoryClassName().equals(that.getFactoryClassName())
                : that.getFactoryClassName() != null) {
            return false;
        }
        if (getProperties() != null ? !getProperties().equals(that.getProperties()) : that.getProperties() != null) {
            return false;
        }
        if (getStoreImplementation() != null ? !getStoreImplementation().equals(that.getStoreImplementation())
                : that.getStoreImplementation() != null) {
            return false;
        }
        return getFactoryImplementation() != null ? getFactoryImplementation().equals(that.getFactoryImplementation())
                : that.getFactoryImplementation() == null;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final int hashCode() {
        int result = (isEnabled() ? 1 : 0);
        result = 31 * result + (getClassName() != null ? getClassName().hashCode() : 0);
        result = 31 * result + (getFactoryClassName() != null ? getFactoryClassName().hashCode() : 0);
        result = 31 * result + (getProperties() != null ? getProperties().hashCode() : 0);
        result = 31 * result + (getStoreImplementation() != null ? getStoreImplementation().hashCode() : 0);
        result = 31 * result + (getFactoryImplementation() != null ? getFactoryImplementation().hashCode() : 0);
        return result;
    }
}
