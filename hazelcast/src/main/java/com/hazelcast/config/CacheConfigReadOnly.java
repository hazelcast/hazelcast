/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

public class CacheConfigReadOnly<K, V> extends CacheConfig<K, V> {

    CacheConfigReadOnly(CacheConfig config) {
        super(config);
    }

//    public CacheConfig setName(String name) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setBackupCount(int backupCount) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setAsyncBackupCount(int asyncBackupCount) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setEvictionPercentage(int evictionPercentage) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
//
//    public CacheConfig setStatisticsEnabled(boolean statisticsEnabled) {
//        throw new UnsupportedOperationException("This config is read-only cache: " + getName());
//    }
}
