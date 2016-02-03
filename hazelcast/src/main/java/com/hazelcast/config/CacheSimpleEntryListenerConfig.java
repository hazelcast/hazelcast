/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
        if (readOnly == null) {
            readOnly = new CacheSimpleEntryListenerConfigReadOnly(this);
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

    /**
     * If old value is required, previously assigned values for the affected keys
     * will be sent to this cache entry listener implementation.
     *
     * @return True if old value is required, false otherwise.
     */
    public boolean isOldValueRequired() {
        return oldValueRequired;
    }

    /**
     * If true, previously assigned values for the affected keys will be sent to this
     * cache entry listener implementation. Setting this attribute to true
     * creates additional traffic. Default value is false.
     *
     * @param oldValueRequired True to have old value required, false otherwise.
     */
    public void setOldValueRequired(boolean oldValueRequired) {
        this.oldValueRequired = oldValueRequired;
    }

    /**
     * If true, this cache entry listener implementation will be called
     * in a synchronous manner.
     *
     * @return True if this cache entry listener implementation will be called
     * in a synchronous manner, false otherwise.
     */
    public boolean isSynchronous() {
        return synchronous;
    }

    /**
     * If true, this cache entry listener implementation will be called
     * in a synchronous manner.
     *
     * @param synchronous True to have this cache entry listener implementation called
     *                    in a synchronous manner, false otherwise.
     */
    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }
}
