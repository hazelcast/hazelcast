/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.tenantcontrol.TenantControl;

/**
 * Accessor for CacheConfig fields
 */
@PrivateApi
public final class CacheConfigAccessor {

    private CacheConfigAccessor() {
    }

    public static <K, V> TenantControl getTenantControl(CacheConfig<K, V> cacheConfig) {
        return cacheConfig.getTenantControl();
    }

    public static <K, V> void setTenantControl(CacheConfig<K, V> cacheConfig, TenantControl tenantControl) {
        cacheConfig.setTenantControl(tenantControl);
    }
}
