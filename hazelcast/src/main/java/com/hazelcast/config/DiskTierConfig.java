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

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.config.LocalDeviceConfig.DEFAULT_DEVICE_NAME;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Disk tier configuration of Tiered-Store.
 *
 * @since 5.1
 */
public class DiskTierConfig implements IdentifiedDataSerializable {

    private boolean enabled;

    private String deviceName = DEFAULT_DEVICE_NAME;

    public DiskTierConfig() {

    }

    public DiskTierConfig(DiskTierConfig diskTierConfig) {
        enabled = diskTierConfig.isEnabled();
        deviceName = diskTierConfig.getDeviceName();
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
     * Returns the device name of this disk tier.
     *
     * @return device name.
     */
    public String getDeviceName() {
        return deviceName;
    }

    /**
     * Sets the device name for this disk tier.
     *
     * @param deviceName device name.
     * @return this DiskTierConfig
     */
    public DiskTierConfig setDeviceName(@Nonnull String deviceName) {
        this.deviceName = checkNotNull(deviceName, "Device name must not be null");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeString(deviceName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        deviceName = in.readString();
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
        return Objects.equals(deviceName, that.deviceName);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (deviceName != null ? deviceName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DiskTierConfig{"
                + "enabled=" + enabled
                + ", deviceName='" + deviceName + '\''
                + '}';
    }
}
