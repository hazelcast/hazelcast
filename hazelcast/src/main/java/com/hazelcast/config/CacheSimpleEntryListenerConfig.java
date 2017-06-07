/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Simple configuration to hold parsed listener config.
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

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
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
     * @return {@code true} if old value is required, {@code false} otherwise
     */
    public boolean isOldValueRequired() {
        return oldValueRequired;
    }

    /**
     * If {@code true}, previously assigned values for the affected keys will be sent to this
     * cache entry listener implementation. Setting this attribute to {@code true}
     * creates additional traffic. Default value is {@code false}.
     *
     * @param oldValueRequired {@code true} to have old value required, {@code false} otherwise
     */
    public void setOldValueRequired(boolean oldValueRequired) {
        this.oldValueRequired = oldValueRequired;
    }

    /**
     * If {@code true}, this cache entry listener implementation will be called in a synchronous manner.
     *
     * @return {@code true} if this cache entry listener implementation will be called in a synchronous manner,
     * {@code false} otherwise
     */
    public boolean isSynchronous() {
        return synchronous;
    }

    /**
     * If {@code true}, this cache entry listener implementation will be called in a synchronous manner.
     *
     * @param synchronous {@code true} to have this cache entry listener implementation called in a synchronous manner,
     *                    {@code false} otherwise.
     */
    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }
}
