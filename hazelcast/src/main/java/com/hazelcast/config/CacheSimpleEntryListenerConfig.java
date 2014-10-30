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
 * Simple configuration to hold parsed listener config
 */
public class CacheSimpleEntryListenerConfig {

    private String cacheEntryListenerFactory;
    private String cacheEntryEventFilterFactory;
    private boolean oldValueRequired;
    private boolean synchronous;

    private CacheSimpleEntryListenerConfigReadOnly readOnly;

    public CacheSimpleEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        this.cacheEntryEventFilterFactory = listenerConfig.cacheEntryEventFilterFactory;
        this.cacheEntryListenerFactory = listenerConfig.cacheEntryListenerFactory;
        this.oldValueRequired = listenerConfig.oldValueRequired;
        this.synchronous = listenerConfig.synchronous;
    }

    public CacheSimpleEntryListenerConfig() {
    }

    public CacheSimpleEntryListenerConfigReadOnly getAsReadOnly() {
        if(readOnly == null) {
            readOnly =new CacheSimpleEntryListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    public String getCacheEntryListenerFactory() {
        return cacheEntryListenerFactory;
    }

    public void setCacheEntryListenerFactory(String cacheEntryListenerFactory) {
        this.cacheEntryListenerFactory = cacheEntryListenerFactory;
    }

    public String getCacheEntryEventFilterFactory() {
        return cacheEntryEventFilterFactory;
    }

    public void setCacheEntryEventFilterFactory(String cacheEntryEventFilterFactory) {
        this.cacheEntryEventFilterFactory = cacheEntryEventFilterFactory;
    }

    public boolean isOldValueRequired() {
        return oldValueRequired;
    }

    public void setOldValueRequired(boolean oldValueRequired) {
        this.oldValueRequired = oldValueRequired;
    }

    public boolean isSynchronous() {
        return synchronous;
    }

    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }
}
