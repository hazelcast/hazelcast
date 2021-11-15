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
    public static final String DEFAULT_DEVICE_BASE_DIR = "tiered-store-device";

    private String deviceName;
    private File baseDir = new File(DEFAULT_DEVICE_BASE_DIR).getAbsoluteFile();

    public DeviceConfig() {

    }

    public DeviceConfig(DeviceConfig deviceConfig) {
        deviceName = deviceConfig.getDeviceName();
        baseDir = deviceConfig.getBaseDir();
    }

    public String getDeviceName() {
        return deviceName;
    }

    public DeviceConfig setDeviceName(String deviceName) {
        this.deviceName = deviceName;
        return this;
    }

    public File getBaseDir() {
        return baseDir;
    }

    public DeviceConfig setBaseDir(File baseDir) {
        this.baseDir = baseDir;
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

        if (!Objects.equals(deviceName, that.deviceName)) {
            return false;
        }
        return Objects.equals(baseDir, that.baseDir);
    }

    @Override
    public final int hashCode() {
        int result = deviceName != null ? deviceName.hashCode() : 0;
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DeviceConfig{"
                + "deviceName='" + deviceName + '\''
                + ", baseDir=" + baseDir
                + '}';
    }
}
