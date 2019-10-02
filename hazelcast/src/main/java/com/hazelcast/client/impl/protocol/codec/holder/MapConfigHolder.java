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

package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.config.MapConfig;

public class MapConfigHolder {
    private String inMemoryFormat;
    private int backupCount;
    private int asyncBackupCount;
    private int timeToLiveSeconds;
    private int maxIdleSeconds;
    private int maxSize;
    private String maxSizePolicy;
    private boolean readBackupData;
    private String evictionPolicy;
    private String mergePolicy;

    public MapConfigHolder(String inMemoryFormat, int backupCount, int asyncBackupCount,
                           int timeToLiveSeconds, int maxIdleSeconds, int maxSize,
                           String maxSizePolicy, boolean readBackupData, String evictionPolicy,
                           String mergePolicy) {
        this.inMemoryFormat = inMemoryFormat;
        this.backupCount = backupCount;
        this.asyncBackupCount = asyncBackupCount;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.maxSize = maxSize;
        this.maxSizePolicy = maxSizePolicy;
        this.readBackupData = readBackupData;
        this.evictionPolicy = evictionPolicy;
        this.mergePolicy = mergePolicy;
    }

    public String getInMemoryFormat() {
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

    public String getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public static MapConfigHolder of(MapConfig mapConfig) {
        return new MapConfigHolder(mapConfig.getInMemoryFormat().name(), mapConfig.getBackupCount(),
                mapConfig.getAsyncBackupCount(), mapConfig.getTimeToLiveSeconds(),
                mapConfig.getMaxIdleSeconds(), mapConfig.getMaxSizeConfig().getSize(),
                mapConfig.getMaxSizeConfig().getMaxSizePolicy().name(), mapConfig.isReadBackupData(),
                mapConfig.getEvictionPolicy().name(), mapConfig.getMergePolicyConfig().getPolicy());
    }
}
