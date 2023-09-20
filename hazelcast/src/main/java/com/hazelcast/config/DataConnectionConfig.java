/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A configuration of a data connection that can be used:
 * <ul>
 *     <li>to create a source or sink in a Jet {@link Pipeline},
 *     <li>as a data connection for {@link MapStore} and {@link MapLoader},
 *     <li>to create an SQL mapping.
 * </ul>
 *
 * @since 5.3
 */
@Beta
public class DataConnectionConfig implements IdentifiedDataSerializable, NamedConfig {

    private String name;
    private String type;
    private boolean shared = true;
    private Properties properties = new Properties();

    public DataConnectionConfig() {
    }

    public DataConnectionConfig(DataConnectionConfig config) {
        name = config.name;
        type = config.type;
        shared = config.shared;
        properties.putAll(config.getProperties());
    }

    public DataConnectionConfig(String name) {
        this.name = checkNotNull(name, "Name must not be null");
    }

    /**
     * Sets the name of this data connection, the name must be unique.
     *
     * @return this DataConnectionConfig
     */
    @Override
    public DataConnectionConfig setName(String name) {
        this.name = checkHasText(name, "Data connection name must contain text");
        return this;
    }

    /**
     * Returns the name of this data connection.
     *
     * @return the name of this data connection
     */
    public String getName() {
        return name;
    }


    /**
     * Returns the type of the {@link DataConnection}
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of the {@link DataConnection}
     */
    public DataConnectionConfig setType(@Nonnull String type) {
        this.type = checkHasText(type, "Data connection type must contain text");
        return this;
    }

    /**
     * {@code true} if an instance of the data connection will be reused. {@code false} when on each usage
     * the data connection instance should be created. The default is {@code true}
     *
     * @return if the data connection instance should be reused
     */
    public boolean isShared() {
        return shared;
    }

    /**
     * {@code true} if an instance of the data connection will be reused. {@code false} when on each usage
     * the data connection instance should be created
     *
     * @param shared if the data connection instance should be reused
     * @return this DataConnectionConfig
     */
    public DataConnectionConfig setShared(boolean shared) {
        this.shared = shared;
        return this;
    }

    /**
     * Returns all the properties of a data connection
     *
     * @return all the properties of a data connection
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Returns a single property of a data connection
     *
     * @param key the property key of a data connection
     * @return property value or null if the given key doesn't exist
     */
    @Nullable
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Returns a single property of a data connection
     *
     * @param key the property key of a data connection
     * @return property value or default value if the given key doesn't exist
     */
    @Nullable
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    /**
     * Sets a single property. See {@link DataConnectionConfig#setProperties(Properties)}
     *
     * @param key   the property key
     * @param value the property value
     * @return this DataConnectionConfig
     */
    public DataConnectionConfig setProperty(String key, String value) {
        properties.setProperty(key, value);
        return this;

    }

    /**
     * Sets the properties of a data connection. See implementations of {@link DataConnection}
     * for supported values
     *
     * @param properties the properties to be set
     * @return this DataConnectionConfig
     */
    public DataConnectionConfig setProperties(Properties properties) {
        this.properties = checkNotNull(properties, "Data connection properties cannot be null, they can be empty");
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataConnectionConfig that = (DataConnectionConfig) o;
        return shared == that.shared && Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, shared, properties);
    }

    @Override
    public String toString() {
        return "DataConnectionConfig{"
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", shared=" + shared
                + ", properties=" + properties
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(shared);
        out.writeInt(properties.size());
        for (String key : properties.stringPropertyNames()) {
            out.writeString(key);
            out.writeString(properties.getProperty(key));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        shared = in.readBoolean();
        int propertiesSize = in.readInt();
        for (int i = 0; i < propertiesSize; i++) {
            String key = in.readString();
            String value = in.readString();
            properties.setProperty(key, value);
        }
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DATA_CONNECTION_CONFIG;
    }
}
