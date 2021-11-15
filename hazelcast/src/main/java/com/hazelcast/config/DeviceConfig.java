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

import java.io.File;
import java.util.Objects;

/**
 * Device configuration for the Tiered-Store.
 *
 * @since 5.1
 */
public class DeviceConfig {

    /**
     * Default base directory for the device.
     */
    public static final String DEFAULT_DEVICE_BASE_DIR = "tiered-store";

    /**
     * Default block/sector size in bytes.
     */
    public static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 4096;

    /**
     * Default device name.
     */
    public static final String DEFAULT_DEVICE_NAME = "local-disk";

    private String deviceName = DEFAULT_DEVICE_NAME;
    private File baseDir = new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile();
    private int blockSize = DEFAULT_BLOCK_SIZE_IN_BYTES;

    public DeviceConfig() {

    }

    public DeviceConfig(DeviceConfig deviceConfig) {
        deviceName = deviceConfig.getDeviceName();
        baseDir = deviceConfig.getBaseDir();
    }

    /**
     * Returns the device name.
     */
    public String getDeviceName() {
        return deviceName;
    }

    /**
     * Sets the device name.
     *
     * @param deviceName device name
     * @return this DeviceConfig
     */
    public DeviceConfig setDeviceName(String deviceName) {
        this.deviceName = deviceName;
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
    public DeviceConfig setBaseDir(File baseDir) {
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
     * @return this DeviceConfig
     */
    public DeviceConfig setBlockSize(int blockSize) {
        this.blockSize = blockSize;
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
        if (!Objects.equals(deviceName, that.deviceName)) {
            return false;
        }
        return Objects.equals(baseDir, that.baseDir);
    }

    @Override
    public final int hashCode() {
        int result = deviceName != null ? deviceName.hashCode() : 0;
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + blockSize;
        return result;
    }

    @Override
    public String toString() {
        return "DeviceConfig{"
                + "deviceName='" + deviceName + '\''
                + ", baseDir=" + baseDir
                + ", blockSize=" + blockSize
                + '}';
    }
}
