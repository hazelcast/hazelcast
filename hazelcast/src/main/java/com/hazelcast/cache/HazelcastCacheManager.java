package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;

public abstract class HazelcastCacheManager implements CacheManager {

    protected HazelcastInstance hazelcastInstance;
    protected CachingProvider cachingProvider;

    protected final HashMap<String, ICache<?, ?>> caches = new HashMap<String, ICache<?, ?>>();

    protected final URI uri;
    protected final WeakReference<ClassLoader> classLoaderReference;
    protected final Properties properties;

    protected final boolean isDefaultURI;
    protected final boolean isDefaultClassLoader;

    protected volatile boolean closeTriggered=false;
    protected final String cacheNamePrefix;

    public HazelcastCacheManager(CachingProvider cachingProvider,URI uri, ClassLoader classLoader, Properties properties) {
        if (cachingProvider == null) {
            throw new NullPointerException("CachingProvider missing");
        }
        this.cachingProvider = cachingProvider;

        isDefaultURI = uri == null || cachingProvider.getDefaultURI().equals(uri);
        this.uri = isDefaultURI ? cachingProvider.getDefaultURI(): uri;

        isDefaultClassLoader = classLoader == null || cachingProvider.getDefaultClassLoader().equals(classLoader);
        final ClassLoader _classLoader = isDefaultClassLoader ? cachingProvider.getDefaultClassLoader() : classLoader;
        this.classLoaderReference = new WeakReference<ClassLoader>(_classLoader);

        this.properties = properties == null ? new Properties() : new Properties(properties);

        this.cacheNamePrefix = cacheNamePrefix();
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
    public <K, V> ICache<K, V> getCache(String cacheName) {
        if (isClosed()) {
            throw new IllegalStateException();
        }
        synchronized (caches) {
            final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
            ICache<?, ?> cache = caches.get(cacheNameWithPrefix);

            if(cache != null) {
                Configuration<?, ?> configuration = cache.getConfiguration(CacheConfig.class);

                if (Object.class.equals(configuration.getKeyType()) && Object.class.equals(configuration.getValueType())){
                    return (ICache<K, V>) cache;
                } else {
                    throw new IllegalArgumentException("Cache " + cacheName + " was " +
                            "defined with specific types Cache<" +
                            configuration.getKeyType() + ", " + configuration.getValueType() + "> " +
                            "in which case CacheManager.getCache(String, Class, Class) must be used");
                }
            }
        }
        return null;
    }

//    @Override
//    public void enableManagement(String cacheName, boolean enabled) {
//        if (isClosed()) {
//            throw new IllegalStateException();
//        }
//        if (cacheName == null) {
//            throw new NullPointerException();
//        }
//        synchronized (caches) {
//            final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
//            ICache<?, ?> cache = caches.get(cacheNameWithPrefix);
//            cache.enableManagement(enabled);
//        }
//    }
//
//    @Override
//    public void enableStatistics(String cacheName, boolean enabled) {
//        if (isClosed()) {
//            throw new IllegalStateException();
//        }
//        if (cacheName == null) {
//            throw new NullPointerException();
//        }
//        synchronized (caches) {
//            final String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
//            ICache<?, ?> cache = caches.get(cacheNameWithPrefix);
//            cache.setStatisticsEnabled(enabled);
//        }
//
////        final CacheConfig configuration = cache.getConfiguration(CacheConfig.class);
////        configuration.setStatisticsEnabled(true);
//    }


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

    protected String cacheNamePrefix(){
        final StringBuilder sb = new StringBuilder("/hz");
        if(!isDefaultClassLoader){
            sb.append("/").append(classLoaderReference.get().toString() );
        }
        if(!isDefaultURI){
            sb.append("/").append(uri.toASCIIString() );
        }
        sb.append("/");
        return sb.toString();
    }

    protected String getCacheNameWithPrefix(String name){
        return cacheNamePrefix + name;
    }
}
