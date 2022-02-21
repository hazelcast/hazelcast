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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Simple configuration to hold parsed listener config.
 */
public class CacheSimpleEntryListenerConfig implements IdentifiedDataSerializable {

    private String cacheEntryListenerFactory;
    private String cacheEntryEventFilterFactory;
    private boolean oldValueRequired;
    private boolean synchronous;

    public CacheSimpleEntryListenerConfig(CacheSimpleEntryListenerConfig listenerConfig) {
        this.cacheEntryEventFilterFactory = listenerConfig.cacheEntryEventFilterFactory;
        this.cacheEntryListenerFactory = listenerConfig.cacheEntryListenerFactory;
        this.oldValueRequired = listenerConfig.oldValueRequired;
        this.synchronous = listenerConfig.synchronous;
    }

    public CacheSimpleEntryListenerConfig() {
    }

    public String getCacheEntryListenerFactory() {
        return cacheEntryListenerFactory;
    }

    public CacheSimpleEntryListenerConfig setCacheEntryListenerFactory(String cacheEntryListenerFactory) {
        this.cacheEntryListenerFactory = cacheEntryListenerFactory;
        return this;
    }

    public String getCacheEntryEventFilterFactory() {
        return cacheEntryEventFilterFactory;
    }

    public CacheSimpleEntryListenerConfig setCacheEntryEventFilterFactory(String cacheEntryEventFilterFactory) {
        this.cacheEntryEventFilterFactory = cacheEntryEventFilterFactory;
        return this;
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
     * @return this configuration
     */
    public CacheSimpleEntryListenerConfig setOldValueRequired(boolean oldValueRequired) {
        this.oldValueRequired = oldValueRequired;
        return this;
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
     * @return this configuration
     */
    public CacheSimpleEntryListenerConfig setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheSimpleEntryListenerConfig)) {
            return false;
        }

        CacheSimpleEntryListenerConfig that = (CacheSimpleEntryListenerConfig) o;

        if (oldValueRequired != that.oldValueRequired) {
            return false;
        }
        if (synchronous != that.synchronous) {
            return false;
        }
        if (cacheEntryListenerFactory != null ? !cacheEntryListenerFactory.equals(that.cacheEntryListenerFactory)
                : that.cacheEntryListenerFactory != null) {
            return false;
        }
        return cacheEntryEventFilterFactory != null
                ? cacheEntryEventFilterFactory.equals(that.cacheEntryEventFilterFactory)
                : that.cacheEntryEventFilterFactory == null;
    }

    @Override
    public final int hashCode() {
        int result = cacheEntryListenerFactory != null ? cacheEntryListenerFactory.hashCode() : 0;
        result = 31 * result + (cacheEntryEventFilterFactory != null ? cacheEntryEventFilterFactory.hashCode() : 0);
        result = 31 * result + (oldValueRequired ? 1 : 0);
        result = 31 * result + (synchronous ? 1 : 0);
        return result;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.SIMPLE_CACHE_ENTRY_LISTENER_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(cacheEntryEventFilterFactory);
        out.writeString(cacheEntryListenerFactory);
        out.writeBoolean(oldValueRequired);
        out.writeBoolean(synchronous);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        cacheEntryEventFilterFactory = in.readString();
        cacheEntryListenerFactory = in.readString();
        oldValueRequired = in.readBoolean();
        synchronous = in.readBoolean();
    }

    @Override
    public String toString() {
        return "CacheSimpleEntryListenerConfig{"
                + "cacheEntryListenerFactory='" + cacheEntryListenerFactory + '\''
                + ", cacheEntryEventFilterFactory='" + cacheEntryEventFilterFactory + '\''
                + ", oldValueRequired=" + oldValueRequired
                + ", synchronous=" + synchronous
                + '}';
    }
}
