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

/**
 * Readonly version of CacheSimpleEntryListenerConfig
 */
public class CacheSimpleEntryListenerConfigReadOnly
        extends CacheSimpleEntryListenerConfig {

    public CacheSimpleEntryListenerConfigReadOnly(CacheSimpleEntryListenerConfig listenerConfig) {
        super(listenerConfig);
    }

    @Override
    public void setSynchronous(boolean synchronous) {
        super.setSynchronous(synchronous);
    }

    @Override
    public void setOldValueRequired(boolean oldValueRequired) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setCacheEntryEventFilterFactory(String cacheEntryEventFilterFactory) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setCacheEntryListenerFactory(String cacheEntryListenerFactory) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
