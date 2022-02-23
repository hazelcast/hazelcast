/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Objects;

/**
 * Tiered-Store configuration.
 *
 * @since 5.1
 */
public class TieredStoreConfig implements IdentifiedDataSerializable {

    /**
     * Default value for if tiered-store is enabled.
     */
    public static final boolean DEFAULT_ENABLED = false;

    private boolean enabled = DEFAULT_ENABLED;
    private MemoryTierConfig memoryTierConfig = new MemoryTierConfig();
    private DiskTierConfig diskTierConfig = new DiskTierConfig();

    public TieredStoreConfig() {

    }

    public TieredStoreConfig(TieredStoreConfig tieredStoreConfig) {
        this.enabled = tieredStoreConfig.isEnabled();
        this.memoryTierConfig = new MemoryTierConfig(tieredStoreConfig.getMemoryTierConfig());
        this.diskTierConfig = new DiskTierConfig(tieredStoreConfig.getDiskTierConfig());
    }

    /**
     * Returns whether tiered-store is enabled on the related data structure.
     *
     * @return true if tiered-store is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether tiered-store is enabled on the related data structure.
     *
     * @param enabled enabled parameter.
     * @return this TieredStoreConfig
     */
    public TieredStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the memory tier config of this tiered-store configuration.
     *
     * @return {@code MemoryTierConfig} of this tiered-store configuration.
     */
    public MemoryTierConfig getMemoryTierConfig() {
        return memoryTierConfig;
    }

    /**
     * Sets the memory tier config of this tiered-store configuration.
     *
     * @param memoryTierConfig memory tier configuration.
     * @return this TieredStoreConfig
     */
    public TieredStoreConfig setMemoryTierConfig(MemoryTierConfig memoryTierConfig) {
        this.memoryTierConfig = memoryTierConfig;
        return this;
    }

    /**
     * Returns the disk tier config of this tiered-store configuration.
     *
     * @return {@code TSDiskTierConfig} of this tiered-store configuration.
     */
    public DiskTierConfig getDiskTierConfig() {
        return diskTierConfig;
    }

    /**
     * Sets the disk tier config of this tiered-store configuration.
     *
     * @param diskTierConfig disk tier configuration.
     * @return this TieredStoreConfig
     */
    public TieredStoreConfig setDiskTierConfig(DiskTierConfig diskTierConfig) {
        this.diskTierConfig = diskTierConfig;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TieredStoreConfig)) {
            return false;
        }

        TieredStoreConfig that = (TieredStoreConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (!Objects.equals(memoryTierConfig, that.memoryTierConfig)) {
            return false;
        }
        return Objects.equals(diskTierConfig, that.diskTierConfig);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (memoryTierConfig != null ? memoryTierConfig.hashCode() : 0);
        result = 31 * result + (diskTierConfig != null ? diskTierConfig.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TieredStoreConfig{"
                + "enabled=" + enabled
                + ", memoryTierConfig=" + memoryTierConfig
                + ", diskTierConfig=" + diskTierConfig
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeObject(memoryTierConfig);
        out.writeObject(diskTierConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        memoryTierConfig = in.readObject();
        diskTierConfig = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.TIERED_STORE_CONFIG;
    }
}
