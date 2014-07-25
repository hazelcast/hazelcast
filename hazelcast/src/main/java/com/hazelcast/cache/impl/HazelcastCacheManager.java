package com.hazelcast.cache.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;

public class HazelcastCacheManager implements CacheManager {

    protected final HazelcastInstance hazelcastInstance;
    protected final HazelcastCachingProvider cachingProvider;

    private final URI uri;
    private final WeakReference<ClassLoader> classLoaderReference;
    private final Properties properties;

    private volatile boolean closeTriggered=false;


    public HazelcastCacheManager(HazelcastCachingProvider cachingProvider, HazelcastInstance hazelcastInstance, URI uri, ClassLoader classLoader, Properties properties) {
        if (cachingProvider == null) {
            throw new NullPointerException("CachingProvider missing");
        }
        this.cachingProvider = cachingProvider;

        if (hazelcastInstance == null) {
            throw new NullPointerException("hazelcastInstance missing");
        }
        this.hazelcastInstance = hazelcastInstance;

        if (uri == null) {
            throw new NullPointerException("CacheManager URI missing");
        }
        this.uri = uri;

        if (classLoader == null) {
            throw new NullPointerException("ClassLoader missing");
        }
        this.classLoaderReference = new WeakReference<ClassLoader>(classLoader);
        this.properties = properties == null ? new Properties() : new Properties(properties);

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
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        if (isClosed()) {
            throw new IllegalStateException();
        }

        if (cacheName == null) {
            throw new NullPointerException("cacheName must not be null");
        }

        if (configuration == null) {
            throw new NullPointerException("configuration must not be null");
        }
        //FIXME this check has race condition, can not detect proxies created but not emitted to other nodes yet.
        for (String name : getCacheNames()) {
            if (cacheName.equals(name)) {
                throw new CacheException("A cache named " + cacheName + " already exists.");
            }
        }

        final CacheConfig<K, V> cacheConfig;
        if (configuration instanceof CompleteConfiguration) {
            cacheConfig = new CacheConfig<K, V>((CompleteConfiguration) configuration);
        } else {
            cacheConfig = new CacheConfig<K, V>();
            cacheConfig.setStoreByValue(configuration.isStoreByValue());
            cacheConfig.setTypes(configuration.getKeyType(), configuration.getValueType());
        }
        cacheConfig.setName(cacheName);
        hazelcastInstance.getConfig().addCacheConfig(cacheConfig);
        return getCacheObject(cacheName);
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
        final CacheConfig<?, ?> configuration = hazelcastInstance.getConfig().getCacheConfig(cacheName);
        if (configuration.getKeyType() != null && configuration.getKeyType().equals(keyType)) {
            if (configuration.getValueType() != null && configuration.getValueType().equals(valueType)) {
                return getCacheObject(cacheName);
            } else {
                throw new ClassCastException("Incompatible cache value types specified, expected " +
                        configuration.getValueType() + " but " + valueType + " was specified");
            }
        } else {
            throw new ClassCastException("Incompatible cache key types specified, expected " +
                    configuration.getKeyType() + " but " + keyType + " was specified");
        }
    }

    @Override
    public <K, V> ICache<K, V> getCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        final CacheConfig<?, ?> configuration = hazelcastInstance.getConfig().getCacheConfig(cacheName);

        if (Object.class.equals(configuration.getKeyType()) && Object.class.equals(configuration.getValueType())){
            return getCacheObject(cacheName);
        } else {
            throw new IllegalArgumentException("Cache " + cacheName + " was " +
                    "defined with specific types Cache<" +
                    configuration.getKeyType() + ", " + configuration.getValueType() + "> " +
                    "in which case CacheManager.getCache(String, Class, Class) must be used");
        }
    }

    private <K, V> ICache<K, V> getCacheObject(String cacheName){
        ICache<K, V> cache = hazelcastInstance.getDistributedObject(CacheService.SERVICE_NAME, cacheName);
        if (cache != null && cache instanceof CacheProxy) {
            ((CacheProxy<K, V>) cache).initCacheManager(this);
        }
        return cache;
    }

    @Override
    public Iterable<String> getCacheNames() {
        final Collection<String> names;
        if (isClosed()) {
            names = Collections.emptySet();
        } else {
            names = new LinkedHashSet<String>();
            final HazelcastInstance hz = hazelcastInstance;
            Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
            for (DistributedObject distributedObject : distributedObjects) {
                if (distributedObject instanceof ICache) {
                    names.add(distributedObject.getName());
                }
            }
        }
        return Collections.unmodifiableCollection(names);
    }

    @Override
    public void destroyCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        HazelcastInstance hz = hazelcastInstance;
        hazelcastInstance.getConfig().removeCacheConfig(cacheName);
        DistributedObject cache = hz.getDistributedObject(CacheService.SERVICE_NAME, cacheName);
        cache.destroy();
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        final ICache<Object, Object> cache = getCacheObject(cacheName);
        cache.setStatisticsEnabled(enabled);
        final CacheConfig configuration = cache.getConfiguration(CacheConfig.class);
        configuration.setStatisticsEnabled(true);
    }

    @Override
    public void close() {
        if(!closeTriggered){
            HazelcastInstance hz = hazelcastInstance;
            Collection<DistributedObject> distributedObjects = hz.getDistributedObjects();
            for (DistributedObject distributedObject : distributedObjects) {
                if (distributedObject instanceof ICache) {
                    distributedObject.destroy();
                }
            }
            hz.shutdown();
            cachingProvider.releaseCacheManager(uri,classLoaderReference.get());
        }
        closeTriggered=true;
    }

    @Override
    public boolean isClosed() {
        return closeTriggered || !hazelcastInstance.getLifecycleService().isRunning();
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
}
