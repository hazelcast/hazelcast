package com.hazelcast.cache.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;

import javax.cache.Cache;
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
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract {@link CacheManager} implementation provides shared functionality to server and client cache managers.
 * There are two cache managers which can be accessed via their providers.
 * <ul>
 *     <li>Client: HazelcastClientCacheManager.</li>
 *     <li>Server: HazelcastServerCacheManager.</li>
 * </ul>
 * <p>
 *    {@link HazelcastCacheManager} manages the lifecycle of the caches created or accessed through itself.
 * </p>
 * @see CacheManager
 */
//todo AbstractHazelcastCacheManager would be better
public abstract class HazelcastCacheManager implements CacheManager {

    protected final ConcurrentMap<String, ICache<?, ?>> caches = new ConcurrentHashMap<String, ICache<?, ?>>();
    protected final URI uri;
    protected final WeakReference<ClassLoader> classLoaderReference;
    protected final Properties properties;
    protected final String cacheNamePrefix;
    protected final boolean isDefaultURI;
    protected final boolean isDefaultClassLoader;

    protected ILogger logger;
    protected CachingProvider cachingProvider;
    protected HazelcastInstance hazelcastInstance;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    public HazelcastCacheManager(CachingProvider cachingProvider, URI uri, ClassLoader classLoader, Properties properties) {
        if (cachingProvider == null) {
            throw new NullPointerException("CachingProvider missing");
        }
        this.cachingProvider = cachingProvider;

        isDefaultURI = uri == null || cachingProvider.getDefaultURI().equals(uri);
        this.uri = isDefaultURI ? cachingProvider.getDefaultURI() : uri;

        isDefaultClassLoader = classLoader == null || cachingProvider.getDefaultClassLoader().equals(classLoader);
        final ClassLoader localClassLoader = isDefaultClassLoader ? cachingProvider.getDefaultClassLoader() : classLoader;
        this.classLoaderReference = new WeakReference<ClassLoader>(localClassLoader);

        this.properties = properties == null ? new Properties() : new Properties(properties);

        this.cacheNamePrefix = cacheNamePrefix();
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        //TODO: WARNING important method, handles dynamic cache config
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }
        if (configuration == null) {
            throw new NullPointerException("configuration must not be null");
        }
        //CREATE THE CONFIG ON PARTITION
        final CacheConfig<K, V> newCacheConfig = createCacheConfig(cacheName, configuration);
        //create proxy object
        final ICache<K, V> cacheProxy = createCacheProxy(newCacheConfig);
        final boolean created = createConfigOnPartition(newCacheConfig);
        if (created) {
            //single thread region because createConfigOnPartition is single threaded by partition thread
            //UPDATE LOCAL MEMBER
            addCacheConfigIfAbsentToLocal(newCacheConfig);
            //no need to a putIfAbsent as this is a single threaded region
            caches.put(newCacheConfig.getNameWithPrefix(), cacheProxy);
            //REGISTER LISTENERS
            registerListeners(newCacheConfig, cacheProxy);
            return cacheProxy;
        } else {
            final ICache<?, ?> entries = caches.putIfAbsent(newCacheConfig.getNameWithPrefix(), cacheProxy);
            if (entries == null) {
                //REGISTER LISTENERS
                registerListeners(newCacheConfig, cacheProxy);
                return cacheProxy;
            }
        }
        throw new CacheException("A cache named " + cacheName + " already exists.");
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
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (keyType == null) {
            throw new NullPointerException("keyType can not be null");
        }
        if (valueType == null) {
            throw new NullPointerException("valueType can not be null");
        }
        final ICache<?, ?> cache = getCacheUnchecked(cacheName);
        if (cache != null) {
            Configuration<?, ?> configuration = cache.getConfiguration(CacheConfig.class);
            if (configuration.getKeyType() != null && configuration.getKeyType().equals(keyType)) {
                if (configuration.getValueType() != null && configuration.getValueType().equals(valueType)) {
                    return (ICache<K, V>) cache;
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

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        final ICache<?, ?> cache = getCacheUnchecked(cacheName);
        if (cache != null) {
            Configuration<?, ?> configuration = cache.getConfiguration(CacheConfig.class);

            if (Object.class.equals(configuration.getKeyType()) && Object.class.equals(configuration.getValueType())) {
                return (ICache<K, V>) cache;
            } else {
                throw new IllegalArgumentException(
                        "Cache " + cacheName + " was " + "defined with specific types Cache<" + configuration.getKeyType() + ", "
                                + configuration.getValueType() + "> "
                                + "in which case CacheManager.getCache(String, Class, Class) must be used");
            }
        }
        return null;
    }

    protected <K, V> ICache<?, ?> getCacheUnchecked(String cacheName) {
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICache<?, ?> cache = caches.get(cacheNameWithPrefix);
        if (cache == null) {
            //FIXME review getCache
            CacheConfig<K, V> cacheConfig = null;
            if (cacheConfig == null) {
                //remote check
                cacheConfig = getCacheConfigFromPartition(cacheNameWithPrefix);
            }
            if (cacheConfig == null) {
                //no cache found
                return null;
            }
            //create the cache proxy which already exists in the cluster
            final ICache<K, V> cacheProxy = createCacheProxy(cacheConfig);
            final ICache<?, ?> iCache = caches.putIfAbsent(cacheNameWithPrefix, cacheProxy);
            cache = iCache != null ? iCache : cacheProxy;
        }
        return cache;
    }

    @Override
    public Iterable<String> getCacheNames() {
        Set<String> names;
        if (isClosed()) {
            names = Collections.emptySet();
        } else {
            names = new LinkedHashSet<String>();
            for (String nameWithPrefix : caches.keySet()) {
                final String name = nameWithPrefix.substring(nameWithPrefix.indexOf(cacheNamePrefix) + cacheNamePrefix.length());
                names.add(name);
            }
        }
        return Collections.unmodifiableCollection(names);
    }

    @Override
    public void destroyCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        final ICache<?, ?> cache = caches.remove(cacheNameWithPrefix);
        if (cache != null) {
            cache.destroy();
        }
        removeCacheConfigFromLocal(cacheNameWithPrefix);
    }

    /**
     * Removes the local copy of the cache configuration if one exists.
     * Default implementation does not require it. But client implementation overrides this to track a local copy
     * of the config.
     * @param cacheName cache name.
     */
    protected void removeCacheConfigFromLocal(String cacheName) {
    }

    @Override
    public void close() {
        if (isDestroyed.get() || !isClosed.compareAndSet(false, true)) {
            return;
        }
        for (ICache cache : caches.values()) {
            cache.close();
        }
        postClose();
        //TODO do we need to clear it
        //        caches.clear();
    }

    protected abstract void postClose();

    /**
     * Destroy all managed caches.
     */
    public void destroy() {
        if (!isDestroyed.compareAndSet(false, true)) {
            return;
        }
        isClosed.set(true);
        for (ICache cache : caches.values()) {
            cache.destroy();
        }
        caches.clear();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get() || !hazelcastInstance.getLifecycleService().isRunning();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(HazelcastCacheManager.class)) {
            return (T) this;
        }
        throw new IllegalArgumentException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastCacheManager{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append(", cachingProvider=").append(cachingProvider);
        sb.append('}');
        return sb.toString();
    }

    /**
     * This method calculates a fixed string based on the URI and classloader using the formula:
     * <p>/hz[/uri][/classloader]/</p>
     * <p>URI and classloader are dropped if they have default values.</p>
     * @return the calculated cache prefix.
     */
    protected String cacheNamePrefix() {
        final StringBuilder sb = new StringBuilder("/hz");
        final ClassLoader classLoader = getClassLoader();
        if (!isDefaultClassLoader && classLoader != null) {
            sb.append("/").append(classLoader.toString());
        }
        if (!isDefaultURI) {
            sb.append("/").append(uri.toASCIIString());
        }
        sb.append("/");
        return sb.toString();
    }

    protected String getCacheNameWithPrefix(String name) {
        return cacheNamePrefix + name;
    }

    protected <K, V, C extends Configuration<K, V>> CacheConfig<K, V> createCacheConfig(String cacheName, C configuration) {
        final CacheConfig<K, V> cacheConfig;
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

    protected abstract <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName);

    protected abstract <K, V> boolean createConfigOnPartition(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> void addCacheConfigIfAbsentToLocal(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName);

    protected <K, V> void registerListeners(CacheConfig<K, V> cacheConfig, ICache<K, V> source) {
        //REGISTER LISTENERS
        final Iterator<CacheEntryListenerConfiguration<K, V>> iterator = cacheConfig.getCacheEntryListenerConfigurations()
                .iterator();
        while (iterator.hasNext()) {
            final CacheEntryListenerConfiguration<K, V> listenerConfig = iterator.next();
            iterator.remove();
            source.registerCacheEntryListener(listenerConfig);
        }
    }

}
