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

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Device configuration for the Tiered-Store.
 *
 * @since 5.1
 */
public class DeviceConfig implements NamedConfig {

    /**
     * Default base directory for the device.
     */
    public static final String DEFAULT_DEVICE_BASE_DIR = "tiered-store";

    /**
     * Default block/sector size in bytes.
     */
    public static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 4096;

    /**
     * Default read IO thread count.
     */
    public static final int DEFAULT_READ_IO_THREAD_COUNT = 4;

    /**
     * Default write IO thread count.
     */
    public static final int DEFAULT_WRITE_IO_THREAD_COUNT = 4;

    private String name;
    private File baseDir = new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile();
    private int blockSize = DEFAULT_BLOCK_SIZE_IN_BYTES;
    private int readIOThreadCount = DEFAULT_READ_IO_THREAD_COUNT;
    private int writeIOThreadCount = DEFAULT_WRITE_IO_THREAD_COUNT;

    public DeviceConfig() {

    }

    public DeviceConfig(DeviceConfig deviceConfig) {
        name = deviceConfig.getName();
        baseDir = deviceConfig.getBaseDir();
        blockSize = deviceConfig.getBlockSize();
        readIOThreadCount = deviceConfig.getReadIOThreadCount();
        writeIOThreadCount = deviceConfig.getWriteIOThreadCount();
    }

    /**
     * Returns the device name.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Sets the device name.
     *
     * @param name device name
     * @return this DeviceConfig
     */
    @Override
    public DeviceConfig setName(@Nonnull String name) {
        this.name = checkNotNull(name, "Device name must not be null");
        return this;
    }

    /**
     * Base directory for this device.
     * Can be an absolute or relative path to the node startup directory.
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets the base directory for this device.
     *
     * @param baseDir base directory.
     * @return this DeviceConfig
     */
    public DeviceConfig setBaseDir(@Nonnull File baseDir) {
        this.baseDir = checkNotNull(baseDir, "Base directory must not be null");
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
     * @return this DeviceConfig
     */
    public DeviceConfig setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    /**
     * Returns the read IO thread count;
     *
     * @return read IO thread count
     */
    public int getReadIOThreadCount() {
        return readIOThreadCount;
    }

    /**
     * Sets the read IO thread count.
     *
     * @param readIOThreadCount read IO thread count
     * @return this DeviceConfig
     */
    public DeviceConfig setReadIOThreadCount(int readIOThreadCount) {
        this.readIOThreadCount = readIOThreadCount;
        return this;
    }

    /**
     * Returns the write IO thread count
     *
     * @return write IO thread count
     */
    public int getWriteIOThreadCount() {
        return writeIOThreadCount;
    }

    /**
     * Sets the write IO thread count.
     *
     * @param writeIOThreadCount write IO thread count
     * @return this DeviceConfig
     */
    public DeviceConfig setWriteIOThreadCount(int writeIOThreadCount) {
        this.writeIOThreadCount = writeIOThreadCount;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeviceConfig)) {
            return false;
        }

        DeviceConfig that = (DeviceConfig) o;

        if (blockSize != that.blockSize) {
            return false;
        }
        if (readIOThreadCount != that.readIOThreadCount) {
            return false;
        }
        if (writeIOThreadCount != that.writeIOThreadCount) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        return Objects.equals(baseDir, that.baseDir);
    }

    @Override
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + blockSize;
        result = 31 * result + readIOThreadCount;
        result = 31 * result + writeIOThreadCount;
        return result;
    }

    @Override
    public String toString() {
        return "DeviceConfig{"
                + "name='" + name + '\''
                + ", baseDir=" + baseDir
                + ", blockSize=" + blockSize
                + ", readIOThreadCount=" + readIOThreadCount
                + ", writeIOThreadCount=" + writeIOThreadCount
                + '}';
    }
}
