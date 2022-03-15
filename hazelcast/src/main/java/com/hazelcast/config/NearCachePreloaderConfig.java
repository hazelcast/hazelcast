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

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for storing and pre-loading Near Cache keys.
 *
 * Preloader re-populates Near Cache after client/server restart to provide
 * fast access.
 *
 * @since 3.8
 */
@SuppressWarnings("WeakerAccess")
public class NearCachePreloaderConfig implements IdentifiedDataSerializable, Serializable {

    /**
     * Default initial delay for the Near Cache key storage.
     */
    public static final int DEFAULT_STORE_INITIAL_DELAY_SECONDS = 600;

    /**
     * Default interval for the Near Cache key storage (in seconds).
     */
    public static final int DEFAULT_STORE_INTERVAL_SECONDS = 600;

    private boolean enabled;
    private String directory = "";
    private int storeInitialDelaySeconds = DEFAULT_STORE_INITIAL_DELAY_SECONDS;
    private int storeIntervalSeconds = DEFAULT_STORE_INTERVAL_SECONDS;

    public NearCachePreloaderConfig() {
    }

    public NearCachePreloaderConfig(NearCachePreloaderConfig nearCachePreloaderConfig) {
        this(nearCachePreloaderConfig.enabled, nearCachePreloaderConfig.directory);
        this.storeInitialDelaySeconds = nearCachePreloaderConfig.storeInitialDelaySeconds;
        this.storeIntervalSeconds = nearCachePreloaderConfig.storeIntervalSeconds;
    }

    public NearCachePreloaderConfig(String directory) {
        this(true, directory);
    }

    public NearCachePreloaderConfig(boolean enabled, String directory) {
        this.enabled = enabled;
        this.directory = checkNotNull(directory, "directory cannot be null!");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NearCachePreloaderConfig setEnabled(boolean isEnabled) {
        this.enabled = isEnabled;
        return this;
    }

    public NearCachePreloaderConfig setDirectory(String directory) {
        this.directory = checkNotNull(directory, "directory cannot be null!");
        return this;
    }

    public String getDirectory() {
        return directory;
    }

    public int getStoreInitialDelaySeconds() {
        return storeInitialDelaySeconds;
    }

    public NearCachePreloaderConfig setStoreInitialDelaySeconds(int storeInitialDelaySeconds) {
        this.storeInitialDelaySeconds = checkPositive("storeInitialDelaySeconds",
                storeInitialDelaySeconds);
        return this;
    }

    public int getStoreIntervalSeconds() {
        return storeIntervalSeconds;
    }

    public NearCachePreloaderConfig setStoreIntervalSeconds(int storeIntervalSeconds) {
        this.storeIntervalSeconds = checkPositive("storeIntervalSeconds", storeIntervalSeconds);
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.NEAR_CACHE_PRELOADER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeString(directory);
        out.writeInt(storeInitialDelaySeconds);
        out.writeInt(storeIntervalSeconds);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        directory = in.readString();
        storeInitialDelaySeconds = in.readInt();
        storeIntervalSeconds = in.readInt();
    }

    @Override
    public String toString() {
        return "NearCachePreloaderConfig{"
                + "enabled=" + enabled
                + ", directory=" + directory
                + ", storeInitialDelaySeconds=" + storeInitialDelaySeconds
                + ", storeIntervalSeconds=" + storeIntervalSeconds
                + '}';
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NearCachePreloaderConfig that = (NearCachePreloaderConfig) o;
        if (enabled != that.enabled) {
            return false;
        }
        if (storeInitialDelaySeconds != that.storeInitialDelaySeconds) {
            return false;
        }
        if (storeIntervalSeconds != that.storeIntervalSeconds) {
            return false;
        }
        return directory != null ? directory.equals(that.directory) : that.directory == null;
    }

    @Override
    public int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (directory != null ? directory.hashCode() : 0);
        result = 31 * result + storeInitialDelaySeconds;
        result = 31 * result + storeIntervalSeconds;
        return result;
    }
}
