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

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

/**
 * interface of MutableConfiguration for jcache
 */
public interface CacheConfiguration<K, V>
        extends CompleteConfiguration<K, V> {


    /**
     * Sets the expected type of keys and values for a {@link javax.cache.Cache}
     * configured with this {@link javax.cache.configuration.Configuration}. Setting both to
     * <code>Object.class</code> means type-safety checks are not required.
     * <p>
     * This is used by {@link javax.cache.CacheManager} to ensure that the key and value
     * types are the same as those configured for the {@link javax.cache.Cache} prior to
     * returning a requested cache from this method.
     * <p>
     * Implementations may further perform type checking on mutative cache operations
     * and throw a {@link ClassCastException} if these checks fail.
     *
     * @param keyType   the expected key type
     * @param valueType the expected value type
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     * @throws NullPointerException should the key or value type be null
     * @see javax.cache.CacheManager#getCache(String, Class, Class)
     */
    CacheConfiguration<K, V> setTypes(Class<K> keyType, Class<V> valueType);

    /**
     * Add a configuration for a {@link javax.cache.event.CacheEntryListener}.
     *
     * @param cacheEntryListenerConfiguration the
     *  {@link javax.cache.configuration.CacheEntryListenerConfiguration}
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     * @throws IllegalArgumentException is the same CacheEntryListenerConfiguration
     * is used more than once
     */
    CacheConfiguration<K, V> addCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

    /**
     * Remove a configuration for a {@link javax.cache.event.CacheEntryListener}.
     *
     * @param cacheEntryListenerConfiguration  the
     *     {@link javax.cache.configuration.CacheEntryListenerConfiguration} to remove
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> removeCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

    /**
     * Set the {@link javax.cache.integration.CacheLoader} factory.
     *
     * @param factory the {@link javax.cache.integration.CacheLoader} {@link javax.cache.configuration.Factory}
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends
            CacheLoader<K, V>> factory);

    /**
     * Set the {@link javax.cache.integration.CacheWriter} factory.
     *
     * @param factory the {@link javax.cache.integration.CacheWriter} {@link javax.cache.configuration.Factory}
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setCacheWriterFactory(Factory<? extends
            CacheWriter<? super K, ? super V>> factory);

    /**
     * Set the {@link javax.cache.configuration.Factory} for the {@link javax.cache.expiry.ExpiryPolicy}.  If <code>null</code>
     * is specified the default {@link javax.cache.expiry.ExpiryPolicy} is used.
     *
     * @param factory the {@link javax.cache.expiry.ExpiryPolicy} {@link javax.cache.configuration.Factory}
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends
            ExpiryPolicy> factory);

    /**
     * Set if read-through caching should be used.
     * <p>
     * It is an invalid configuration to set this to true without specifying a
     * {@link javax.cache.integration.CacheLoader} {@link javax.cache.configuration.Factory}.
     *
     * @param isReadThrough <code>true</code> if read-through is required
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setReadThrough(boolean isReadThrough);

    /**
     * Set if write-through caching should be used.
     * <p>
     * It is an invalid configuration to set this to true without specifying a
     * {@link javax.cache.integration.CacheWriter} {@link javax.cache.configuration.Factory}.
     *
     * @param isWriteThrough <code>true</code> if write-through is required
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough);

    /**
     * Set if a configured cache should use store-by-value or store-by-reference
     * semantics.
     *
     * @param isStoreByValue <code>true</code> if store-by-value is required,
     *                       <code>false</code> for store-by-reference
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setStoreByValue(boolean isStoreByValue);


    /**
     * Sets whether statistics gathering is enabled on a cache.
     * <p>
     * Statistics may be enabled or disabled at runtime via
     * {@link javax.cache.CacheManager#enableStatistics(String, boolean)}.
     *
     * @param enabled true to enable statistics, false to disable.
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled);


    /**
     * Sets whether management is enabled on a cache.
     * <p>
     * Management may be enabled or disabled at runtime via
     * {@link javax.cache.CacheManager#enableManagement(String, boolean)}.
     *
     * @param enabled true to enable statistics, false to disable.
     * @return the {@link com.hazelcast.config.CacheConfiguration} to permit fluent-style method calls
     */
    CacheConfiguration<K, V> setManagementEnabled(boolean enabled);

}
