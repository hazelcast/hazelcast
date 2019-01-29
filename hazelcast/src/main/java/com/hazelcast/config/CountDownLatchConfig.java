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
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.ICountDownLatch}.
 *
 * @since 3.10
 */
public class CountDownLatchConfig implements IdentifiedDataSerializable, Versioned {

    private transient CountDownLatchConfigReadOnly readOnly;

    private String name;
    private String quorumName;

    /**
     * Creates a default configured {@link CountDownLatchConfig}.
     */
    public CountDownLatchConfig() {
    }

    /**
     * Creates a default configured {@link CountDownLatchConfig} setting its name to the given name.
     *
     * @param name name to set
     */
    public CountDownLatchConfig(String name) {
        setName(name);
    }

    /**
     * Creates a CountDownLatchConfig by cloning another one.
     *
     * @param config the CountDownLatchConfig to copy
     * @throws IllegalArgumentException if config is {@code null}
     */
    public CountDownLatchConfig(CountDownLatchConfig config) {
        isNotNull(config, "config");
        this.name = config.getName();
        this.quorumName = config.getQuorumName();
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public CountDownLatchConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CountDownLatchConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the CountDownLatch. If no name has been configured, {@code null} is returned.
     *
     * @return the name of the CountDownLatch
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the CountDownLatch.
     *
     * @param name the name of the CountDownLatch
     * @return the updated CountDownLatchConfig
     * @throws IllegalArgumentException if name is {@code null} or empty
     */
    public CountDownLatchConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    public String getQuorumName() {
        return quorumName;
    }

    public CountDownLatchConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    @Override
    public String toString() {
        return "CountDownLatchConfig{"
                + "name='" + name + '\''
                + ", quorumName=" + quorumName
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.COUNT_DOWN_LATCH_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(quorumName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        quorumName = in.readUTF();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CountDownLatchConfig)) {
            return false;
        }

        CountDownLatchConfig that = (CountDownLatchConfig) o;
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }

    static class CountDownLatchConfigReadOnly extends CountDownLatchConfig {

        CountDownLatchConfigReadOnly(CountDownLatchConfig config) {
            super(config);
        }

        @Override
        public CountDownLatchConfig setName(String name) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }

        @Override
        public CountDownLatchConfig setQuorumName(String name) {
            throw new UnsupportedOperationException("This is a read-only config!");
        }
    }

}
