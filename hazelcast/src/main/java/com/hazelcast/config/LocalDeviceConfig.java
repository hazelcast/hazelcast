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

import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Local Device configuration for the Tiered-Store.
 *
 * @since 5.1
 */
public final class LocalDeviceConfig implements DeviceConfig {

    /**
     * Default device name.
     */
    public static final String DEFAULT_DEVICE_NAME = "default-tiered-store-device";

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

    /**
     * Default device capacity. It is 256 GB.
     */
    public static final Capacity DEFAULT_CAPACITY = Capacity.of(256, MemoryUnit.GIGABYTES);

    private String name = DEFAULT_DEVICE_NAME;
    private File baseDir = new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile();
    private Capacity capacity = DEFAULT_CAPACITY;
    private int blockSize = DEFAULT_BLOCK_SIZE_IN_BYTES;
    private int readIOThreadCount = DEFAULT_READ_IO_THREAD_COUNT;
    private int writeIOThreadCount = DEFAULT_WRITE_IO_THREAD_COUNT;

    public LocalDeviceConfig() {

    }

    public LocalDeviceConfig(LocalDeviceConfig localDeviceConfig) {
        name = localDeviceConfig.getName();
        baseDir = localDeviceConfig.getBaseDir();
        capacity = localDeviceConfig.getCapacity();
        blockSize = localDeviceConfig.getBlockSize();
        readIOThreadCount = localDeviceConfig.getReadIOThreadCount();
        writeIOThreadCount = localDeviceConfig.getWriteIOThreadCount();
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
    public LocalDeviceConfig setName(@Nonnull String name) {
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
    public LocalDeviceConfig setBaseDir(@Nonnull File baseDir) {
        this.baseDir = checkNotNull(baseDir, "Base directory must not be null");
        return this;
    }

    /**
     * Returns the capacity of this device.
     *
     * @return device capacity.
     */
    @Override
    public Capacity getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of this device.
     *
     * @param capacity capacity.
     * @return this LocalDeviceConfig
     */
    public LocalDeviceConfig setCapacity(Capacity capacity) {
        this.capacity = capacity;
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
    public LocalDeviceConfig setBlockSize(int blockSize) {
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
    public LocalDeviceConfig setReadIOThreadCount(int readIOThreadCount) {
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
    public LocalDeviceConfig setWriteIOThreadCount(int writeIOThreadCount) {
        this.writeIOThreadCount = writeIOThreadCount;
        return this;
    }

    /**
     * Returns {@code true} since {@code this} is a configuration for a local device.
     */
    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalDeviceConfig that = (LocalDeviceConfig) o;

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
        if (!Objects.equals(baseDir, that.baseDir)) {
            return false;
        }
        return Objects.equals(capacity, that.capacity);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + (capacity != null ? capacity.hashCode() : 0);
        result = 31 * result + blockSize;
        result = 31 * result + readIOThreadCount;
        result = 31 * result + writeIOThreadCount;
        return result;
    }

    @Override
    public String toString() {
        return "LocalDeviceConfig{"
                + "name='" + name + '\''
                + ", baseDir=" + baseDir
                + ", capacity=" + capacity
                + ", blockSize=" + blockSize
                + ", readIOThreadCount=" + readIOThreadCount
                + ", writeIOThreadCount=" + writeIOThreadCount
                + '}';
    }
}
