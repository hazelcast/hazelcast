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

package com.hazelcast.internal.nearcache.impl;


import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nullable;

/**
 * Hook to be used by near cache enabled proxy objects.
 *
 * With this hook, you can implement needed logic
 * for truly invalidate/populate local near cache.
 */
public interface NearCachingHook<K, V> {

    NearCachingHook EMPTY_HOOK = new NearCachingHook() {

        @Override
        public void beforeRemoteCall(Object key, Data keyData,
                                     Object value, Data valueData) {
        }

        @Override
        public void onRemoteCallSuccess() {
        }

        @Override
        public void onRemoteCallFailure() {

        }
    };

    void beforeRemoteCall(K key, Data keyData, @Nullable V value, @Nullable Data valueData);

    void onRemoteCallSuccess();

    void onRemoteCallFailure();
}
