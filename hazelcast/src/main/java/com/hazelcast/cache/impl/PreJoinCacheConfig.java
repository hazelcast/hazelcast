/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AbstractCacheConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * This subclass of {@link CacheConfig} is used to communicate cache configurations in pre-join cache operations when cluster
 * version is at least 3.9. The key difference against {@link CacheConfig} is that the key/value class names are used in its
 * serialized form, instead of the actual {@code Class} objects. Thus the actual key-value classes are only resolved when first
 * used (by means of {@link CacheConfig#getKeyType()} or {@link CacheConfig#getValueType()}). This allows resolution of
 * these classes from remote user code deployment repositories (which are not available while the pre-join operation is being
 * deserialized and executed).
 *
 * @param <K> the key type of this cache configuration
 * @param <V> the value type
 * @since 3.9
 */
public class PreJoinCacheConfig<K, V> extends CacheConfig<K, V> implements IdentifiedDataSerializable {

    public PreJoinCacheConfig() {
        super();
    }

    /**
     * Constructor that copies given {@code cacheConfig}'s properties to a new {@link PreJoinCacheConfig}. It is assumed that
     * the given {@code cacheConfig}'s key-value types have already been resolved to loaded classes.
     * @param cacheConfig   the original {@link CacheConfig} to copy into a new {@link PreJoinCacheConfig}
     */
    public PreJoinCacheConfig(CacheConfig cacheConfig) {
        this(cacheConfig, true);
    }

    public PreJoinCacheConfig(CacheConfig cacheConfig, boolean resolved) {
        cacheConfig.copy(this, resolved);
    }

    @Override
    protected void writeKeyValueTypes(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(getKeyClassName());
        out.writeUTF(getValueClassName());
    }

    @Override
    protected void readKeyValueTypes(ObjectDataInput in)
            throws IOException {
        setKeyClassName(in.readUTF());
        setValueClassName(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.PRE_JOIN_CACHE_CONFIG;
    }

    /**
     * @return this configuration as a {@link CacheConfig}
     */
    public CacheConfig asCacheConfig() {
        return this.copy(new CacheConfig(), false);
    }

    @Override
    @SuppressWarnings("checkstyle:illegaltype")
    protected boolean keyValueTypesEqual(AbstractCacheConfig that) {
        if (!this.getKeyClassName().equals(that.getKeyClassName())) {
            return false;
        }

        if (!this.getValueClassName().equals(that.getValueClassName())) {
            return false;
        }

        return true;
    }
}
