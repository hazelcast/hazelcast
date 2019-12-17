/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.management;

import com.hazelcast.client.impl.protocol.codec.MCGetMapConfigCodec.ResponseParameters;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;

public class MCMapConfig {
    private boolean readBackupData;
    private int maxSize;
    private int backupCount;
    private int maxIdleSeconds;
    private int asyncBackupCount;
    private int timeToLiveSeconds;
    private String mergePolicy;
    private MaxSizePolicy maxSizePolicy;
    private EvictionPolicy evictionPolicy;
    private InMemoryFormat inMemoryFormat;

    public static MCMapConfig fromResponse(ResponseParameters params) {
        MCMapConfig config = new MCMapConfig();
        config.inMemoryFormat = InMemoryFormat.getById(params.inMemoryFormat);
        config.backupCount = params.backupCount;
        config.asyncBackupCount = params.asyncBackupCount;
        config.timeToLiveSeconds = params.timeToLiveSeconds;
        config.maxIdleSeconds = params.maxIdleSeconds;
        config.maxSize = params.maxSize;
        config.maxSizePolicy = MaxSizePolicy.getById(params.maxSizePolicy);
        config.readBackupData = params.readBackupData;
        config.evictionPolicy = EvictionPolicy.getById(params.evictionPolicy);
        config.mergePolicy = params.mergePolicy;
        return config;
    }

    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public String getMergePolicy() {
        return mergePolicy;
    }
}
