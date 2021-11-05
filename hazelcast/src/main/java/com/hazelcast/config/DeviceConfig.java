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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DeviceConfig {

    /**
     * Default block size.
     */
    public static final int DEFAULT_BLOCK_SIZE = 4096;
    /**
     * Default device name.
     */
    public static final String DEFAULT_NAME = "hz-device";

    private String name;
    private String baseDir;
    private int blockSize;
    private long capacity;

    public DeviceConfig() {
        blockSize = DEFAULT_BLOCK_SIZE;
        name = DEFAULT_NAME;
    }

    public DeviceConfig(DeviceConfig deviceConfig) {
        requireNonNull(deviceConfig);
        name = deviceConfig.getName();
        baseDir = deviceConfig.getBaseDir();
        blockSize = deviceConfig.getBlockSize();
        capacity = deviceConfig.getCapacity();
    }

    public String getName() {
        return name;
    }

    public DeviceConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public DeviceConfig setBaseDir(String baseDir) {
        this.baseDir = baseDir;
        return this;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public DeviceConfig setBlockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public long getCapacity() {
        return capacity;
    }

    public DeviceConfig setCapacity(long capacity) {
        this.capacity = capacity;
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
        if (capacity != that.capacity) {
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
        result = 31 * result + (int) (capacity ^ (capacity >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "DeviceConfig{"
                + "name='" + name + '\''
                + ", baseDir='" + baseDir + '\''
                + ", blockSize=" + blockSize
                + ", capacity=" + capacity
                + '}';
    }
}
