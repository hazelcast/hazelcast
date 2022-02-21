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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;

public class RingbufferConfigReadOnly extends RingbufferConfig {

    public RingbufferConfigReadOnly(RingbufferConfig config) {
        super(config);
    }

    @Override
    public RingbufferStoreConfig getRingbufferStoreConfig() {
        final RingbufferStoreConfig storeConfig = super.getRingbufferStoreConfig();
        if (storeConfig != null) {
            return new RingbufferStoreConfigReadOnly(storeConfig);
        } else {
            return null;
        }
    }

    @Override
    public RingbufferConfig setCapacity(int capacity) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setAsyncBackupCount(int asyncBackupCount) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setBackupCount(int backupCount) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setRingbufferStoreConfig(RingbufferStoreConfig ringbufferStoreConfig) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw throwReadOnly();
    }

    @Override
    public RingbufferConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
