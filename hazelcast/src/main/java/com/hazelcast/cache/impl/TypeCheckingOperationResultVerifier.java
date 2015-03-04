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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.spi.OperationResultVerifier;

public class TypeCheckingOperationResultVerifier<K, V> implements OperationResultVerifier<V> {

    private final CacheConfig<K, V> cacheConfig;

    public TypeCheckingOperationResultVerifier(CacheConfig<K, V> cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public Object verify(V value) {
        try {
            CacheProxyUtil.validateConfiguredValueType(cacheConfig, value);
            return value;
        } catch (ClassCastException cce) {
            throw cce;
        }
    }
}
