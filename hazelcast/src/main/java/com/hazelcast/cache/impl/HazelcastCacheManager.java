package com.hazelcast.cache.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;

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

public abstract class HazelcastCacheManager
        implements CacheManager {

    protected final ConcurrentMap<String, ICache<?, ?>> caches = new ConcurrentHashMap<String, ICache<?, ?>>();
    protected final URI uri;
    protected final WeakReference<ClassLoader> classLoaderReference;
    protected final Properties properties;
    protected final String cacheNamePrefix;
    protected HazelcastInstance hazelcastInstance;
    protected CachingProvider cachingProvider;

    final boolean isDefaultURI;
    final boolean isDefaultClassLoader;

    //    protected volatile boolean closeTriggered;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

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
        final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        final CacheConfig<K, V> cacheConfig = getCacheConfigLocal(cacheNameWithPrefix);
        if (cacheConfig == null) {
            final CacheConfig<K, V> newCacheConfig = createCacheConfig(cacheName, configuration);
            //CREATE THE CONFIG ON PARTITION BY cacheNamePrefix using a request
            final boolean created = createConfigOnPartition(newCacheConfig);
            if (created) {
                //CREATE ON OTHERS TOO
                createConfigOnAllMembers(newCacheConfig);
                //UPDATE LOCAL MEMBER
                addCacheConfigIfAbsentToLocal(newCacheConfig);
                //create proxy object
                final ICache<K, V> cacheProxy = createCacheProxy(newCacheConfig);
                final ICache<?, ?> entries = caches.putIfAbsent(cacheNameWithPrefix, cacheProxy);
                if(entries == null){
                    if (newCacheConfig.isStatisticsEnabled()) {
                        enableStatistics(cacheName, true);
                    }
                    if (newCacheConfig.isManagementEnabled()) {
                        enableManagement(cacheName, true);
                    }
                    //REGISTER LISTENERS
                    registerListeners(newCacheConfig, cacheProxy);
                    return cacheProxy;
                }
            } else {
                //this node don't have the config, so grep it and spread that one to cluster
                final CacheConfig<K, V> cacheConfigFromPartition = getCacheConfigFromPartition(cacheNameWithPrefix);
                //ADD CONFIG ON EACH NODE
                createConfigOnAllMembers(cacheConfigFromPartition);
                //UPDATE LOCAL MEMBER
                addCacheConfigIfAbsentToLocal(cacheConfigFromPartition);
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
            CacheConfig<K, V> cacheConfig = getCacheConfigLocal(cacheNameWithPrefix);
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
        //TODO implementation decision: should this return all cluster names, or just the managed ones
/* OPTION 1: */
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
        //        return Collections.unmodifiableCollection(caches.keySet());
/* OPTION 2:*/
        //TODO see above todo comment for this code block
        //        Set<String> names;
        //        if (isClosed()) {
        //            names = Collections.emptySet();
        //        } else {
        //            names = new LinkedHashSet<String>();
        //            for(String nameWithPrefix:cacheService.getCacheNames()){
        //                final String name = nameWithPrefix.substring
        //                             (nameWithPrefix.indexOf(cacheNamePrefix)+cacheNamePrefix.length());
        //                names.add(name);
        //            }
        //        }
        //        return Collections.unmodifiableCollection(names);
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
            cache.close();
        }
        removeCacheConfigFromLocal(cacheNameWithPrefix);
    }

    protected void removeCacheConfigFromLocal(String cacheName) {
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        releaseCacheManager(uri, classLoaderReference.get());
        for (ICache cache : caches.values()) {
            cache.close();
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

    protected void releaseCacheManager(URI uri, ClassLoader classLoader) {
        ((HazelcastAbstractCachingProvider) cachingProvider).releaseCacheManager(uri, classLoader);
    }

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

    protected abstract <K, V> void createConfigOnAllMembers(CacheConfig<K, V> cacheConfig);

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
