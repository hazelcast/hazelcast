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

public class TieredStoreConfig {

    private HybridLogConfig hybridLogConfig;
    private DeviceConfig deviceConfig;
    private boolean enabled;

    public TieredStoreConfig() {
        hybridLogConfig = new HybridLogConfig();
        deviceConfig = new DeviceConfig();
    }

    public TieredStoreConfig(TieredStoreConfig tieredStoreConfig) {
        requireNonNull(tieredStoreConfig);
        enabled = tieredStoreConfig.isEnabled();
        hybridLogConfig = new HybridLogConfig(tieredStoreConfig.getHybridLogConfig());
        deviceConfig = new DeviceConfig(tieredStoreConfig.getDeviceConfig());
    }

    public HybridLogConfig getHybridLogConfig() {
        return hybridLogConfig;
    }

    public TieredStoreConfig setHybridLogConfig(HybridLogConfig hybridLogConfig) {
        this.hybridLogConfig = hybridLogConfig;
        return this;
    }

    public DeviceConfig getDeviceConfig() {
        return deviceConfig;
    }

    public TieredStoreConfig setDeviceConfig(DeviceConfig deviceConfig) {
        this.deviceConfig = deviceConfig;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TieredStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public boolean equals(Object o) {
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
        if (!Objects.equals(hybridLogConfig, that.hybridLogConfig)) {
            return false;
        }
        return Objects.equals(deviceConfig, that.deviceConfig);
    }

    @Override
    public int hashCode() {
        int result = hybridLogConfig != null ? hybridLogConfig.hashCode() : 0;
        result = 31 * result + (deviceConfig != null ? deviceConfig.hashCode() : 0);
        result = 31 * result + (enabled ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TieredStoreConfig{"
                + "hybridLogConfig=" + hybridLogConfig
                + ", deviceConfig=" + deviceConfig
                + ", enabled=" + enabled
                + '}';
    }
}
