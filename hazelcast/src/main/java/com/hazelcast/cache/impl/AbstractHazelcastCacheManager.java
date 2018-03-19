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

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheUtil.getPrefix;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SetUtil.createLinkedHashSet;

/**
 * <p>
 * Abstract {@link HazelcastCacheManager} (also {@link CacheManager} as indirect) implementation
 * provides shared functionality to server and client cache managers.
 * There are two cache managers which can be accessed via their providers.
 * <ul>
 * <li>Client: HazelcastClientCacheManager.</li>
 * <li>Server: HazelcastServerCacheManager.</li>
 * </ul>
 * </p>
 * <p>
 * {@link AbstractHazelcastCacheManager} manages the lifecycle of the caches created or accessed through itself.
 * </p>
 *
 * @see HazelcastCacheManager
 * @see CacheManager
 */
public abstract class AbstractHazelcastCacheManager
        implements HazelcastCacheManager {

    protected final ConcurrentMap<String, ICacheInternal<?, ?>> caches =
            new ConcurrentHashMap<String, ICacheInternal<?, ?>>();
    protected final URI uri;
    protected final WeakReference<ClassLoader> classLoaderReference;
    protected final Properties properties;
    protected final String cacheNamePrefix;
    protected final boolean isDefaultURI;
    protected final boolean isDefaultClassLoader;

    protected final CachingProvider cachingProvider;
    protected final HazelcastInstance hazelcastInstance;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final String lifecycleListenerRegistrationId;

    public AbstractHazelcastCacheManager(CachingProvider cachingProvider, HazelcastInstance hazelcastInstance,
                                         URI uri, ClassLoader classLoader, Properties properties) {
        checkNotNull(cachingProvider, "CachingProvider missing");
        this.cachingProvider = cachingProvider;

        checkNotNull(hazelcastInstance, "hazelcastInstance cannot be null");
        this.hazelcastInstance = hazelcastInstance;

        this.isDefaultURI = uri == null || cachingProvider.getDefaultURI().equals(uri);
        this.uri = isDefaultURI ? cachingProvider.getDefaultURI() : uri;

        this.isDefaultClassLoader = classLoader == null || cachingProvider.getDefaultClassLoader().equals(classLoader);
        ClassLoader localClassLoader = isDefaultClassLoader ? cachingProvider.getDefaultClassLoader() : classLoader;
        this.classLoaderReference = new WeakReference<ClassLoader>(localClassLoader);

        this.properties = properties == null ? new Properties() : new Properties(properties);

        this.cacheNamePrefix = cacheNamePrefix();

        this.lifecycleListenerRegistrationId = registerLifecycleListener();
    }

    private <K, V, C extends Configuration<K, V>> ICacheInternal<K, V> createCacheInternal(String cacheName, C configuration)
            throws IllegalArgumentException {
        ensureOpen();
        checkNotNull(cacheName, "cacheName must not be null");
        checkNotNull(configuration, "configuration must not be null");

        CacheConfig<K, V> newCacheConfig = createCacheConfig(cacheName, configuration);
        checkCacheConfig(newCacheConfig.getInMemoryFormat(), newCacheConfig.getEvictionConfig(),
                newCacheConfig.isStatisticsEnabled(), newCacheConfig.getMergePolicy());

        if (caches.containsKey(newCacheConfig.getNameWithPrefix())) {
            throw new CacheException("A cache named '" + cacheName + "' already exists.");
        }
        // Create cache config on all nodes as sync
        createCacheConfig(cacheName, newCacheConfig, true, true);
        // Create cache proxy object with cache config
        ICacheInternal<K, V> cacheProxy = createCacheProxy(newCacheConfig);
        // Add created cache config to local configurations map
        addCacheConfigIfAbsent(newCacheConfig);
        ICacheInternal<?, ?> existingCache = caches.putIfAbsent(newCacheConfig.getNameWithPrefix(), cacheProxy);
        if (existingCache == null) {
            // Register listeners on new cache
            registerListeners(newCacheConfig, cacheProxy);
            return cacheProxy;
        } else {
            CacheConfig config = existingCache.getConfiguration(CacheConfig.class);
            if (config.equals(newCacheConfig)) {
                return (ICacheInternal<K, V>) existingCache;
            } else {
                throw new CacheException("A cache named " + cacheName + " already exists.");
            }
        }
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Override
    public <K, V, C extends Configuration<K, V>> ICache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        return createCacheInternal(cacheName, configuration);
    }

    private ICacheInternal<?, ?> getOrPutIfAbsent(String nameWithPrefix, ICacheInternal cacheProxy) {
        ICacheInternal<?, ?> cache = caches.get(nameWithPrefix);
        if (cache == null) {
            ICacheInternal<?, ?> iCache = caches.putIfAbsent(nameWithPrefix, cacheProxy);
            cache = iCache != null ? iCache : cacheProxy;
        }
        return cache;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return cachingProvider;
    }

    @Override
    public URI getURI() {
        return this.uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoaderReference.get();
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        ensureOpen();
        checkNotNull(keyType, "keyType can not be null");
        checkNotNull(valueType, "valueType can not be null");
        ICacheInternal<?, ?> cache = getCacheUnchecked(cacheName);
        if (cache != null) {
            Configuration<?, ?> configuration = cache.getConfiguration(CacheConfig.class);
            if (configuration.getKeyType() != null && configuration.getKeyType().equals(keyType)) {
                if (configuration.getValueType() != null && configuration.getValueType().equals(valueType)) {
                    return ensureOpenIfAvailable((ICacheInternal<K, V>) cache);
                } else {
                    throw new ClassCastException(
                            "Incompatible cache value types specified, expected " + configuration.getValueType() + " but "
                                    + valueType + " was specified");
                }
            } else {
                throw new ClassCastException(
                        "Incompatible cache key types specified, expected " + configuration.getKeyType() + " but " + keyType
                                + " was specified");
            }
        }
        return null;
    }

    public <K, V> ICache<K, V> getOrCreateCache(String cacheName, CacheConfig<K, V> cacheConfig) {
        ensureOpen();
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICacheInternal<?, ?> cache = caches.get(cacheNameWithPrefix);
        if (cache == null) {
            cache = createCacheInternal(cacheName, cacheConfig);
        }
        return ensureOpenIfAvailable((ICacheInternal<K, V>) cache);
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        ensureOpen();
        ICacheInternal<?, ?> cache = getCacheUnchecked(cacheName);
        if (cache != null) {
            return ensureOpenIfAvailable((ICacheInternal<K, V>) cache);
        }
        return null;
    }

    protected <K, V> ICacheInternal<K, V> ensureOpenIfAvailable(ICacheInternal<K, V> cache) {
        if (cache != null && cache.isClosed() && !cache.isDestroyed()) {
            cache.open();
        }
        return cache;
    }

    protected <K, V> ICacheInternal<?, ?> getCacheUnchecked(String cacheName) {
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICacheInternal<?, ?> cache = caches.get(cacheNameWithPrefix);
        if (cache == null) {
            CacheConfig<K, V> cacheConfig = findCacheConfig(cacheNameWithPrefix, cacheName, true, true);
            if (cacheConfig == null) {
                // No cache found
                return null;
            }
            // Create the cache proxy which already exists in the cluster as config
            ICacheInternal<K, V> cacheProxy = createCacheProxy(cacheConfig);
            // Put created cache config.
            addCacheConfigIfAbsent(cacheConfig);
            cache = caches.putIfAbsent(cacheNameWithPrefix, cacheProxy);
            if (cache == null) {
                registerListeners(cacheConfig, cacheProxy);
                cache = cacheProxy;
            }
        }
        return cache;
    }

    @Override
    public Iterable<String> getCacheNames() {
        ensureOpen();
        Set<String> names;
        names = createLinkedHashSet(caches.size());
        for (Map.Entry<String, ICacheInternal<?, ?>> entry : caches.entrySet()) {
            String nameWithPrefix = entry.getKey();
            int index = nameWithPrefix.indexOf(cacheNamePrefix) + cacheNamePrefix.length();
            final String name = nameWithPrefix.substring(index);
            names.add(name);
        }
        return Collections.unmodifiableCollection(names);
    }

    @Override
    public void destroyCache(String cacheName) {
        removeCache(cacheName, true);
    }

    @Override
    public void removeCache(String cacheName, boolean destroy) {
        ensureOpen();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICacheInternal<?, ?> cache = caches.remove(cacheNameWithPrefix);
        if (cache != null && destroy) {
            cache.destroy();
        }
        removeCacheConfigFromLocal(cacheNameWithPrefix);
    }

    /**
     * Removes the local copy of the cache configuration if one exists.
     * Default implementation does not require it. But client implementation overrides this to track a local copy
     * of the config.
     *
     * @param cacheNameWithPrefix cache name.
     */
    protected void removeCacheConfigFromLocal(String cacheNameWithPrefix) {
    }

    private String registerLifecycleListener() {
        return hazelcastInstance.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    onShuttingDown();
                }
            }
        });
    }

    private void deregisterLifecycleListener() {
        LifecycleService lifecycleService = hazelcastInstance.getLifecycleService();
        try {
            lifecycleService.removeLifecycleListener(lifecycleListenerRegistrationId);
        } catch (HazelcastInstanceNotActiveException e) {
            // if hazelcastInstance is terminated already,
            // `lifecycleService.removeLifecycleListener` will throw HazelcastInstanceNotActiveException.
            // We can safely ignore this exception. See TerminatedLifecycleService.
            ignore(e);
        }
    }

    @Override
    public void close() {
        if (isDestroyed.get() || !isClosed.compareAndSet(false, true)) {
            return;
        }

        deregisterLifecycleListener();
        for (ICacheInternal cache : caches.values()) {
            cache.close();
        }
        postClose();
        // TODO: do we need to clear it as "caches.clear();"?
    }

    /**
     * Destroy all managed caches.
     */
    @Override
    public void destroy() {
        if (!isDestroyed.compareAndSet(false, true)) {
            return;
        }

        deregisterLifecycleListener();
        for (ICacheInternal cache : caches.values()) {
            cache.destroy();
        }
        caches.clear();

        isClosed.set(true);

        postDestroy();
    }

    protected void postDestroy() {
    }

    @Override
    public boolean isClosed() {
        return isClosed.get() || !hazelcastInstance.getLifecycleService().isRunning();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("CacheManager " + cacheNamePrefix + " is already closed.");
        }
    }

    /**
     * This method calculates a fixed string based on the URI and classloader using the formula:
     * <p>/hz[/uri][/classloader]/</p>
     * <p>URI and classloader are dropped if they have default values.</p>
     *
     * @return the calculated cache prefix.
     */
    protected String cacheNamePrefix() {
        String cacheNamePrefix = getPrefix(
                isDefaultURI ? null : uri,
                isDefaultClassLoader ? null : getClassLoader());
        if (cacheNamePrefix == null) {
            return HazelcastCacheManager.CACHE_MANAGER_PREFIX;
        } else {
            return HazelcastCacheManager.CACHE_MANAGER_PREFIX + cacheNamePrefix;
        }
    }

    @Override
    public String getCacheNameWithPrefix(String name) {
        return cacheNamePrefix + name;
    }

    protected <K, V, C extends Configuration<K, V>> CacheConfig<K, V> createCacheConfig(String cacheName,
                                                                                        C configuration) {
        CacheConfig<K, V> cacheConfig;
        if (configuration instanceof CompleteConfiguration) {
            cacheConfig = new CacheConfig<K, V>((CompleteConfiguration) configuration);
        } else {
            cacheConfig = new CacheConfig<K, V>();
            cacheConfig.setStoreByValue(configuration.isStoreByValue());
            final Class<K> keyType = configuration.getKeyType();
            final Class<V> valueType = configuration.getValueType();
            cacheConfig.setTypes(keyType, valueType);
        }
        cacheConfig.setName(cacheName);
        cacheConfig.setManagerPrefix(this.cacheNamePrefix);
        cacheConfig.setUriString(getURI().toString());
        return cacheConfig;
    }

    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {
        Iterator<CacheEntryListenerConfiguration<K, V>> iterator =
                cacheConfig.getCacheEntryListenerConfigurations().iterator();
        while (iterator.hasNext()) {
            CacheEntryListenerConfiguration<K, V> listenerConfig = iterator.next();
            ((ICacheInternal<K, V>) source).registerCacheEntryListener(listenerConfig, false);
        }
    }

    @Override
    public String toString() {
        return "HazelcastCacheManager{hazelcastInstance=" + hazelcastInstance + ", cachingProvider=" + cachingProvider + '}';
    }

    protected abstract <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> ICacheInternal<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> CacheConfig<K, V> findCacheConfig(String cacheName,
                                                                String simpleCacheName,
                                                                boolean createAlsoOnOthers,
                                                                boolean syncCreate);

    protected abstract <K, V> CacheConfig<K, V> createCacheConfig(String cacheName,
                                                                  CacheConfig<K, V> config,
                                                                  boolean createAlsoOnOthers,
                                                                  boolean syncCreate);

    protected abstract <K, V> CacheConfig<K, V> getCacheConfig(String cacheName,
                                                               String simpleCacheName);

    protected abstract void postClose();

    protected abstract void onShuttingDown();

}
