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

package com.hazelcast.internal.config;

import com.hazelcast.config.NearCachePreloaderConfig;

public class NearCachePreloaderConfigReadOnly extends NearCachePreloaderConfig {

    @SuppressWarnings("unused")
    public NearCachePreloaderConfigReadOnly() {
    }

    public NearCachePreloaderConfigReadOnly(NearCachePreloaderConfig nearCachePreloaderConfig) {
        super(nearCachePreloaderConfig);
    }

    @Override
    public NearCachePreloaderConfig setEnabled(boolean isEnabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NearCachePreloaderConfig setDirectory(String directory) {
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

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException("NearCachePreloaderConfigReadOnly is not serializable");
    }
}
