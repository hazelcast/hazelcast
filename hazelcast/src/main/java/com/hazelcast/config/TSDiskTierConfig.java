/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * Disk tier configuration of Tiered-Store.
 *
 * @since 5.1
 */
public class TSDiskTierConfig implements IdentifiedDataSerializable {

    /**
     * Default base directory for the tiered-store.
     */
    public static final String DEFAULT_TSTORE_BASE_DIR = "tstore";

    /**
     * Default block/sector size in bytes.
     */
    public static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 4096;


    private boolean enabled;
    private File baseDir = new File(DEFAULT_TSTORE_BASE_DIR);
    private int blockSize = DEFAULT_BLOCK_SIZE_IN_BYTES;
    private MemorySize capacity;

    public TSDiskTierConfig() {

    }

    public TSDiskTierConfig(TSDiskTierConfig tsDiskTierConfig) {
        enabled = tsDiskTierConfig.isEnabled();
        baseDir = tsDiskTierConfig.getBaseDir();
        blockSize = tsDiskTierConfig.getBlockSize();
        capacity = tsDiskTierConfig.getCapacity();
    }

    /**
     * Returns whether disk tier is enabled on the related tiered-store.
     *
     * @return true if disk tier is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether disk tier is enabled on the related tiered-store.
     *
     * @param enabled enabled parameter.
     * @return this TSDiskTierConfig
     */
    public TSDiskTierConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Base directory for this disk tier.
     * Can be an absolute or relative path to the node startup directory.
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets the base directory for this disk tier.
     *
     * @param baseDir base directory.
     * @return this TSDiskTierConfig
     */
    public TSDiskTierConfig setBaseDir(File baseDir) {
        this.baseDir = baseDir;
        return this;
    }

    /**
     * Returns disk block/sector size in bytes.
     *
     * @return block size
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Sets the disk block/sector size in bytes.
     * @param blockSize block size.
     * @return this TSDiskTierConfig
     */
    public TSDiskTierConfig setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * Returns the capacity of this disk tier.
     *
     * @return disk tier capacity.
     */
    public MemorySize getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of this disk tier.
     *
     * @param capacity capacity.
     * @return this TSDiskTierConfig
     */
    public TSDiskTierConfig setCapacity(MemorySize capacity) {
        this.capacity = capacity;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TSDiskTierConfig)) {
            return false;
        }

        TSDiskTierConfig that = (TSDiskTierConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (blockSize != that.blockSize) {
            return false;
        }
        if (!Objects.equals(baseDir, that.baseDir)) {
            return false;
        }
        return Objects.equals(capacity, that.capacity);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + blockSize;
        result = 31 * result + (capacity != null ? capacity.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TSDiskTierConfig{"
                + "enabled=" + enabled
                + ", baseDir=" + baseDir
                + ", blockSize=" + blockSize
                + ", capacity=" + capacity
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeString(baseDir.getAbsolutePath());
        out.writeInt(blockSize);
        out.writeLong(capacity.bytes());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        baseDir = new File(in.readString()).getAbsoluteFile();
        blockSize = in.readInt();
        capacity = new MemorySize(in.readLong(), MemoryUnit.BYTES);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.TS_DISK_TIER_CONFIG;
    }
}
