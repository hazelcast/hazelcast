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

package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;

import javax.cache.management.CacheMXBean;

public class CacheMXBeanImpl
        implements CacheMXBean {

    private CacheConfig cacheConfig;

    public CacheMXBeanImpl(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public String getKeyType() {
        return cacheConfig.getKeyType().getName();
    }

    @Override
    public String getValueType() {
        return cacheConfig.getValueType().getName();
    }

    @Override
    public boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    @Override
    public boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    @Override
    public boolean isStoreByValue() {
        return cacheConfig.isStoreByValue();
    }

    @Override
    public boolean isStatisticsEnabled() {
        return cacheConfig.isStatisticsEnabled();
    }

    @Override
    public boolean isManagementEnabled() {
        return cacheConfig.isManagementEnabled();
    }
}
