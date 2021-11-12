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
public class DiskTierConfig implements IdentifiedDataSerializable {

    /**
     * Default base directory for the tiered-store.
     */
    public static final String DEFAULT_TIERED_STORE_BASE_DIR = "tiered-store";

    /**
     * Default block/sector size in bytes.
     */
    public static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 4096;

    private boolean enabled;
    private File baseDir = new File(DEFAULT_TIERED_STORE_BASE_DIR).getAbsoluteFile();
    private int blockSize = DEFAULT_BLOCK_SIZE_IN_BYTES;

    public DiskTierConfig() {

    }

    public DiskTierConfig(DiskTierConfig diskTierConfig) {
        enabled = diskTierConfig.isEnabled();
        baseDir = new File(diskTierConfig.getBaseDir().getAbsolutePath());
        blockSize = diskTierConfig.getBlockSize();
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
     * @return this DiskTierConfig
     */
    public DiskTierConfig setEnabled(boolean enabled) {
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
     * @return this DiskTierConfig
     */
    public DiskTierConfig setBaseDir(File baseDir) {
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
     * @return this DiskTierConfig
     */
    public DiskTierConfig setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeString(baseDir.getAbsolutePath());
        out.writeInt(blockSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        baseDir = new File(in.readString()).getAbsoluteFile();
        blockSize = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DISK_TIER_CONFIG;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DiskTierConfig)) {
            return false;
        }

        DiskTierConfig that = (DiskTierConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (blockSize != that.blockSize) {
            return false;
        }
        return Objects.equals(baseDir, that.baseDir);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + blockSize;
        return result;
    }

    @Override
    public String toString() {
        return "DiskTierConfig{"
                + "enabled=" + enabled
                + ", baseDir=" + baseDir
                + ", blockSize=" + blockSize
                + '}';
    }
}
