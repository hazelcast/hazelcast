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

import com.hazelcast.config.CacheSimpleEntryListenerConfig;

/**
 * Readonly version of CacheSimpleEntryListenerConfig
 */
public class CacheSimpleEntryListenerConfigReadOnly extends CacheSimpleEntryListenerConfig {

    public CacheSimpleEntryListenerConfigReadOnly(CacheSimpleEntryListenerConfig listenerConfig) {
        super(listenerConfig);
    }

    @Override
    public CacheSimpleEntryListenerConfig setSynchronous(boolean synchronous) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public CacheSimpleEntryListenerConfig setOldValueRequired(boolean oldValueRequired) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public CacheSimpleEntryListenerConfig setCacheEntryEventFilterFactory(String cacheEntryEventFilterFactory) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public CacheSimpleEntryListenerConfig setCacheEntryListenerFactory(String cacheEntryListenerFactory) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
