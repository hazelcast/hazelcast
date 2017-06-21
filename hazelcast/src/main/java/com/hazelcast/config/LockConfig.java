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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for the {@link com.hazelcast.core.ILock}.
 */
public class LockConfig implements IdentifiedDataSerializable {

    private String name;
    private String quorumName;

    public LockConfig() {
    }

    /**
     * Creates a {@link LockConfig} with the provided name.
     *
     * @param name the name
     * @throws NullPointerException if name is {@code null}
     */
    public LockConfig(String name) {
        this.name = checkNotNull(name, "name can't be null");
    }

    /**
     * Clones a {@link LockConfig}
     *
     * @param config the lock config to clone
     * @throws NullPointerException if config is {@code null}
     */
    public LockConfig(LockConfig config) {
        checkNotNull(config, "config can't be null");
        this.name = config.name;
        this.quorumName = config.quorumName;
    }

    /**
     * Creates a new {@link LockConfig} by cloning an existing config and overriding the name.
     *
     * @param name   the new name
     * @param config the config
     * @throws NullPointerException if name or config is {@code null}
     */
    public LockConfig(String name, LockConfig config) {
        this(config);
        this.name = checkNotNull(name, "name can't be null");
    }

    /**
     * Sets the name of the lock.
     *
     * @param name the name of the lock
     * @return the updated {@link LockConfig}
     * @throws IllegalArgumentException if name is {@code null} or an empty string
     */
    public LockConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Returns the name of the lock.
     *
     * @return the name of the lock
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the quorum name for lock operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for lock operations.
     *
     * @param quorumName the quorum name
     * @return the updated lock configuration
     */
    public LockConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public LockConfig getAsReadOnly() {
        return new LockConfigReadOnly(this);
    }

    @Override
    public String toString() {
        return "LockConfig{"
                + "name='" + name + '\''
                + ", quorumName='" + quorumName + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.LOCK_CONFIG;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LockConfig that = (LockConfig) o;
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return quorumName != null ? quorumName.equals(that.quorumName) : that.quorumName == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }

    /**
     * A readonly version of the {@link LockConfig}.
     */
    private static class LockConfigReadOnly extends LockConfig {

        LockConfigReadOnly(LockConfig config) {
            super(config);
        }

        @Override
        public LockConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only");
        }

        @Override
        public LockConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only");
        }
    }
}
