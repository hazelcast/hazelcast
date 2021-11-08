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

public class TieredStoreConfig {

    /**
     * Default value for if tiered-store is enabled.
     */
    public static final boolean DEFAULT_ENABLED = false;

    private boolean enabled = DEFAULT_ENABLED;
    private TSInMemoryTierConfig inMemoryTierConfig = new TSInMemoryTierConfig();
    private TSDiskTierConfig diskTierConfig = new TSDiskTierConfig();

    public TieredStoreConfig() {

    }

    public TieredStoreConfig(TieredStoreConfig tieredStoreConfig) {
        this.enabled = tieredStoreConfig.isEnabled();
        this.inMemoryTierConfig = tieredStoreConfig.getInMemoryTierConfig();
        this.diskTierConfig = tieredStoreConfig.getDiskTierConfig();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TieredStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public TSInMemoryTierConfig getInMemoryTierConfig() {
        return inMemoryTierConfig;
    }

    public TieredStoreConfig setInMemoryTierConfig(TSInMemoryTierConfig inMemoryTierConfig) {
        this.inMemoryTierConfig = inMemoryTierConfig;
        return this;
    }

    public TSDiskTierConfig getDiskTierConfig() {
        return diskTierConfig;
    }

    public TieredStoreConfig setDiskTierConfig(TSDiskTierConfig diskTierConfig) {
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
        if (!Objects.equals(inMemoryTierConfig, that.inMemoryTierConfig)) {
            return false;
        }
        return Objects.equals(diskTierConfig, that.diskTierConfig);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (inMemoryTierConfig != null ? inMemoryTierConfig.hashCode() : 0);
        result = 31 * result + (diskTierConfig != null ? diskTierConfig.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TieredStoreConfig{"
                + "enabled=" + enabled
                + ", inMemoryTierConfig=" + inMemoryTierConfig
                + ", diskTierConfig=" + diskTierConfig
                + '}';
    }
}
