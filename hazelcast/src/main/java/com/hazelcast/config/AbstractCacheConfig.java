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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Base class for {@link CacheConfig}
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@BinaryInterface
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractCacheConfig<K, V> implements CacheConfiguration<K, V>, DataSerializable {

    private static final String DEFAULT_KEY_VALUE_TYPE = "java.lang.Object";

    /**
     * The {@link CacheEntryListenerConfiguration}s for the {@link javax.cache.configuration.Configuration}.
     */
    protected Set<CacheEntryListenerConfiguration<K, V>> listenerConfigurations;

    /**
     * The {@link javax.cache.configuration.Factory} for the {@link javax.cache.integration.CacheLoader}.
     */
    protected Factory<CacheLoader<K, V>> cacheLoaderFactory;

    /**
     * The {@link Factory} for the {@link javax.cache.integration.CacheWriter}.
     */
    protected Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory;

    /**
     * The {@link Factory} for the {@link javax.cache.expiry.ExpiryPolicy}.
     */
    protected Factory<ExpiryPolicy> expiryPolicyFactory;

    /**
     * A flag indicating if "read-through" mode is required.
     */
    protected boolean isReadThrough;

    /**
     * A flag indicating if "write-through" mode is required.
     */
    protected boolean isWriteThrough;

    /**
     * A flag indicating if statistics gathering is enabled.
     */
    protected boolean isStatisticsEnabled;

    /**
     * A flag indicating if the cache will be store-by-value or store-by-reference.
     */
    protected boolean isStoreByValue;

    /**
     * Whether management is enabled
     */
    protected boolean isManagementEnabled;

    protected HotRestartConfig hotRestartConfig = new HotRestartConfig();

    /**
     * The type of keys for {@link javax.cache.Cache}s configured with this
     * {@link javax.cache.configuration.Configuration}.
     */
    private Class<K> keyType;
    private String keyClassName = DEFAULT_KEY_VALUE_TYPE;

    /**
     * The type of values for {@link javax.cache.Cache}s configured with this
     * {@link javax.cache.configuration.Configuration}.
     */
    private Class<V> valueType;
    private String valueClassName = DEFAULT_KEY_VALUE_TYPE;

    /**
     * The ClassLoader to be used to resolve key & value types, if set
     */
    private transient ClassLoader classLoader;

    public AbstractCacheConfig() {
        this.listenerConfigurations = createConcurrentSet();
        this.cacheLoaderFactory = null;
        this.cacheWriterFactory = null;
        this.expiryPolicyFactory = EternalExpiryPolicy.factoryOf();
        this.isReadThrough = false;
        this.isWriteThrough = false;
        this.isStatisticsEnabled = false;
        this.isStoreByValue = true;
        this.isManagementEnabled = false;
    }

    public AbstractCacheConfig(CompleteConfiguration<K, V> configuration) {
        setKeyType(configuration.getKeyType());
        setValueType(configuration.getValueType());
        this.listenerConfigurations = createConcurrentSet();
        for (CacheEntryListenerConfiguration<K, V> listenerConf : configuration.getCacheEntryListenerConfigurations()) {
            listenerConfigurations.add(listenerConf);
        }
        this.cacheLoaderFactory = configuration.getCacheLoaderFactory();
        this.cacheWriterFactory = configuration.getCacheWriterFactory();


        Factory<ExpiryPolicy> factory = configuration.getExpiryPolicyFactory();
        this.expiryPolicyFactory = factory == null ? EternalExpiryPolicy.factoryOf() : factory;

        this.isReadThrough = configuration.isReadThrough();
        this.isWriteThrough = configuration.isWriteThrough();
        this.isStatisticsEnabled = configuration.isStatisticsEnabled();
        this.isStoreByValue = configuration.isStoreByValue();
        this.isManagementEnabled = configuration.isManagementEnabled();
    }

    /**
     * Add a configuration for a {@link javax.cache.event.CacheEntryListener}.
     *
     * @param cacheEntryListenerConfiguration the {@link CacheEntryListenerConfiguration}
     * @return the {@link CacheConfig}
     * @throws IllegalArgumentException if the same CacheEntryListenerConfiguration
     *                                  is used more than once
     */
    @Override
    public CacheConfiguration<K, V> addCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

        checkNotNull(cacheEntryListenerConfiguration, "CacheEntryListenerConfiguration can't be null");
        if (!listenerConfigurations.add(cacheEntryListenerConfiguration)) {
            throw new IllegalArgumentException("A CacheEntryListenerConfiguration can "
                    + "be registered only once");
        }
        return this;
    }

    /**
     * Remove a configuration for a {@link javax.cache.event.CacheEntryListener}.
     *
     * @param cacheEntryListenerConfiguration the {@link CacheEntryListenerConfiguration} to remove
     * @return the {@link CacheConfig}
     */
    @Override
    public CacheConfiguration<K, V> removeCacheEntryListenerConfiguration(
            CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        checkNotNull(cacheEntryListenerConfiguration, "CacheEntryListenerConfiguration can't be null");
        listenerConfigurations.remove(cacheEntryListenerConfiguration);
        return this;
    }

    @Override
    public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
        return listenerConfigurations;
    }

    @Override
    public boolean isReadThrough() {
        return isReadThrough;
    }

    @Override
    public CacheConfiguration<K, V> setReadThrough(boolean isReadThrough) {
        this.isReadThrough = isReadThrough;
        return this;
    }

    @Override
    public boolean isWriteThrough() {
        return isWriteThrough;
    }

    @Override
    public CacheConfiguration<K, V> setWriteThrough(boolean isWriteThrough) {
        this.isWriteThrough = isWriteThrough;
        return this;
    }

    @Override
    public boolean isStatisticsEnabled() {
        return isStatisticsEnabled;
    }

    /**
     * Sets whether or not statistics gathering is enabled on this cache.
     * <p>
     * Statistics may be enabled or disabled at runtime via {@link javax.cache.CacheManager#enableStatistics(String, boolean)}.
     *
     * @param enabled {@code true} to enable statistics, {@code false} to disable
     * @return the {@link CacheConfig}
     */
    @Override
    public CacheConfiguration<K, V> setStatisticsEnabled(boolean enabled) {
        this.isStatisticsEnabled = enabled;
        return this;
    }

    @Override
    public boolean isManagementEnabled() {
        return isManagementEnabled;
    }

    /**
     * Sets whether or not management is enabled on this cache.
     * <p>
     * Management may be enabled or disabled at runtime via {@link javax.cache.CacheManager#enableManagement(String, boolean)}.
     *
     * @param enabled {@code true} to enable statistics, {@code false} to disable
     * @return the {@link CacheConfig}
     */
    @Override
    public CacheConfiguration<K, V> setManagementEnabled(boolean enabled) {
        this.isManagementEnabled = enabled;
        return this;
    }

    /**
     * Gets the {@code HotRestartConfig} for this {@code CacheConfiguration}
     *
     * @return hot restart config
     */
    public HotRestartConfig getHotRestartConfig() {
        return hotRestartConfig;
    }

    /**
     * Sets the {@code HotRestartConfig} for this {@code CacheConfiguration}
     *
     * @param hotRestartConfig hot restart config
     * @return this {@code CacheConfiguration} instance
     */
    public CacheConfiguration<K, V> setHotRestartConfig(HotRestartConfig hotRestartConfig) {
        this.hotRestartConfig = hotRestartConfig;
        return this;
    }

    @Override
    public Factory<CacheLoader<K, V>> getCacheLoaderFactory() {
        return cacheLoaderFactory;
    }

    @Override
    public CacheConfiguration<K, V> setCacheLoaderFactory(Factory<? extends CacheLoader<K, V>> cacheLoaderFactory) {
        this.cacheLoaderFactory = (Factory<CacheLoader<K, V>>) cacheLoaderFactory;
        return this;
    }

    @Override
    public CacheConfiguration<K, V> setExpiryPolicyFactory(Factory<? extends ExpiryPolicy> expiryPolicyFactory) {
        this.expiryPolicyFactory = (Factory<ExpiryPolicy>) expiryPolicyFactory;
        return this;
    }

    @Override
    public CacheConfiguration<K, V> setCacheWriterFactory(
            Factory<? extends CacheWriter<? super K, ? super V>> cacheWriterFactory) {
        this.cacheWriterFactory = (Factory<CacheWriter<? super K, ? super V>>) cacheWriterFactory;
        return this;
    }

    @Override
    public Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory() {
        return cacheWriterFactory;
    }

    @Override
    public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
        return expiryPolicyFactory;
    }

    @Override
    public Class<K> getKeyType() {
        return keyType != null ? keyType : resolveKeyType();
    }

    public String getKeyClassName() {
        return keyClassName;
    }

    public CacheConfiguration<K, V> setKeyClassName(String keyClassName) {
        this.keyClassName = keyClassName;
        return this;
    }

    @Override
    public Class<V> getValueType() {
        return valueType != null ? valueType : resolveValueType();
    }

    public String getValueClassName() {
        return valueClassName;
    }

    public CacheConfiguration<K, V> setValueClassName(String valueClassName) {
        this.valueClassName = valueClassName;
        return this;
    }

    private Class<K> resolveKeyType() {
        keyType = resolve(keyClassName);
        return keyType;
    }

    private Class<V> resolveValueType() {
        valueType = resolve(valueClassName);
        return valueType;
    }

    private Class resolve(String className) {
        Class type = null;
        if (className != null) {
            try {
                type = ClassLoaderUtil.loadClass(classLoader, className);
            } catch (ClassNotFoundException e) {
                throw new HazelcastException("Could not resolve type " + className, e);
            }
        }
        if (type == null) {
            type = Object.class;
        }
        return type;
    }

    /**
     * Sets the expected type of keys and values for a {@link javax.cache.Cache}
     * configured with this {@link javax.cache.configuration.Configuration}.
     * Setting both to {@code Object.class} means type-safety checks are not required.
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
     * @return the {@link CacheConfig}
     * @throws NullPointerException should the key or value type be null
     * @see javax.cache.CacheManager#getCache(String, Class, Class)
     */
    @Override
    public CacheConfiguration<K, V> setTypes(Class<K> keyType, Class<V> valueType) {
        if (keyType == null || valueType == null) {
            throw new NullPointerException("keyType and/or valueType can't be null");
        }
        setKeyType(keyType);
        setValueType(valueType);
        return this;
    }

    @Override
    public boolean isStoreByValue() {
        return isStoreByValue;
    }

    /**
     * Set if a configured cache should use store-by-value or store-by-reference
     * semantics.
     *
     * @param storeByValue {@code true} if store-by-value is required,
     *                     {@code false} for store-by-reference
     * @return the {@link CacheConfig}
     */
    @Override
    public CacheConfiguration<K, V> setStoreByValue(boolean storeByValue) {
        this.isStoreByValue = storeByValue;
        return this;
    }

    protected Set<CacheEntryListenerConfiguration<K, V>> createConcurrentSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<CacheEntryListenerConfiguration<K, V>, Boolean>());
    }

    public CacheConfiguration<K, V> setKeyType(Class<K> keyType) {
        this.keyType = keyType;
        if (keyType != null) {
            this.keyClassName = keyType.getName();
        }
        return this;
    }

    public CacheConfiguration<K, V> setValueType(Class<V> valueType) {
        this.valueType = valueType;
        if (valueType != null) {
            this.valueClassName = valueType.getName();
        }
        return this;
    }

    public CacheConfiguration<K, V> setListenerConfigurations() {
        this.listenerConfigurations = createConcurrentSet();
        return this;
    }

    @Override
    public int hashCode() {
        int result = cacheLoaderFactory != null ? cacheLoaderFactory.hashCode() : 0;
        result = 31 * result + listenerConfigurations.hashCode();
        result = 31 * result + keyType.hashCode();
        result = 31 * result + valueType.hashCode();
        result = 31 * result + (cacheWriterFactory != null ? cacheWriterFactory.hashCode() : 0);
        result = 31 * result + (expiryPolicyFactory != null ? expiryPolicyFactory.hashCode() : 0);
        result = 31 * result + (isReadThrough ? 1 : 0);
        result = 31 * result + (isWriteThrough ? 1 : 0);
        result = 31 * result + (isStatisticsEnabled ? 1 : 0);
        result = 31 * result + (isStoreByValue ? 1 : 0);
        result = 31 * result + (isManagementEnabled ? 1 : 0);
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof AbstractCacheConfig)) {
            return false;
        }
        AbstractCacheConfig that = (AbstractCacheConfig) o;

        if (isManagementEnabled != that.isManagementEnabled) {
            return false;
        }
        if (isReadThrough != that.isReadThrough) {
            return false;
        }
        if (isStatisticsEnabled != that.isStatisticsEnabled) {
            return false;
        }
        if (isStoreByValue != that.isStoreByValue) {
            return false;
        }
        if (isWriteThrough != that.isWriteThrough) {
            return false;
        }
        if (cacheLoaderFactory != null ? !cacheLoaderFactory.equals(that.cacheLoaderFactory) : that.cacheLoaderFactory != null) {
            return false;
        }
        if (cacheWriterFactory != null ? !cacheWriterFactory.equals(that.cacheWriterFactory) : that.cacheWriterFactory != null) {
            return false;
        }
        if (expiryPolicyFactory != null
                ? !expiryPolicyFactory.equals(that.expiryPolicyFactory) : that.expiryPolicyFactory != null) {
            return false;
        }
        if (!listenerConfigurations.equals(that.listenerConfigurations)) {
            return false;
        }

        return keyValueTypesEqual(that);
    }

    protected void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @SuppressWarnings("checkstyle:illegaltype")
    protected boolean keyValueTypesEqual(AbstractCacheConfig that) {
        if (!getKeyType().equals(that.getKeyType())) {
            return false;
        }

        if (!getValueType().equals(that.getValueType())) {
            return false;
        }

        return true;
    }
}
