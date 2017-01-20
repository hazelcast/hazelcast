/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;
import java.io.Serializable;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configuration for eviction.
 * You can set a limit for number of entries or total memory cost of entries.
 */
@Beta
@BinaryInterface
public class NearCachePreloaderConfig implements DataSerializable, Serializable {

    /**
     * Default initial delay for the Near Cache key storage.
     */
    public static final int DEFAULT_STORE_INITIAL_DELAY_SECONDS = 600;

    /**
     * Default interval for the Near Cache key storage (in seconds).
     */
    public static final int DEFAULT_STORE_INTERVAL_SECONDS = 600;

    private boolean enabled;
    private String filename = "";
    private int storeInitialDelaySeconds = DEFAULT_STORE_INITIAL_DELAY_SECONDS;
    private int storeIntervalSeconds = DEFAULT_STORE_INTERVAL_SECONDS;

    private NearCachePreloaderConfig readOnly;

    public NearCachePreloaderConfig() {
    }

    public NearCachePreloaderConfig(NearCachePreloaderConfig nearCachePreloaderConfig) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        this(nearCachePreloaderConfig.enabled, nearCachePreloaderConfig.filename);
    }

    public NearCachePreloaderConfig(String filename) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        this(true, filename);
    }

    public NearCachePreloaderConfig(boolean enabled, String filename) {
        /**
         * ===== NOTE =====
         *
         * Do not use setters, because they are overridden in the readonly version of this config and
         * they cause an "UnsupportedOperationException". Just set directly if the value is valid.
         */

        this.enabled = enabled;
        this.filename = checkNotNull(filename, "filename cannot be null!");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NearCachePreloaderConfig setEnabled(boolean isEnabled) {
        this.enabled = isEnabled;
        return this;
    }

    public NearCachePreloaderConfig setFilename(String filename) {
        this.filename = checkNotNull(filename, "filename cannot be null!");
        return this;
    }

    public String getFilename() {
        return filename;
    }

    public int getStoreInitialDelaySeconds() {
        return storeInitialDelaySeconds;
    }

    public NearCachePreloaderConfig setStoreInitialDelaySeconds(int storeInitialDelaySeconds) {
        this.storeInitialDelaySeconds = checkPositive(storeInitialDelaySeconds,
                "storeInitialDelaySeconds must be a positive number!");
        return this;
    }

    public int getStoreIntervalSeconds() {
        return storeIntervalSeconds;
    }

    public NearCachePreloaderConfig setStoreIntervalSeconds(int storeIntervalSeconds) {
        this.storeIntervalSeconds = checkPositive(storeIntervalSeconds,
                "storeIntervalSeconds must be a positive number!");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeUTF(filename);
        out.writeInt(storeInitialDelaySeconds);
        out.writeInt(storeIntervalSeconds);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        filename = in.readUTF();
        storeInitialDelaySeconds = in.readInt();
        storeIntervalSeconds = in.readInt();
    }

    @Override
    public String toString() {
        return "NearCachePreloaderConfig{"
                + "enabled=" + enabled
                + ", filename=" + filename
                + ", storeInitialDelaySeconds=" + storeInitialDelaySeconds
                + ", storeIntervalSeconds=" + storeIntervalSeconds
                + '}';
    }

    NearCachePreloaderConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new NearCachePreloaderConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * A readonly version of the {@link NearCachePreloaderConfig}.
     */
    @Beta
    @PrivateApi
    @BinaryInterface
    private static class NearCachePreloaderConfigReadOnly extends NearCachePreloaderConfig {

        NearCachePreloaderConfigReadOnly(NearCachePreloaderConfig nearCachePreloaderConfig) {
            super(nearCachePreloaderConfig);
        }

        @Override
        public NearCachePreloaderConfig setEnabled(boolean isEnabled) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NearCachePreloaderConfig setFilename(String filename) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NearCachePreloaderConfig setStoreInitialDelaySeconds(int storeInitialDelaySeconds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NearCachePreloaderConfig setStoreIntervalSeconds(int storeIntervalSeconds) {
            throw new UnsupportedOperationException();
        }
    }
}
