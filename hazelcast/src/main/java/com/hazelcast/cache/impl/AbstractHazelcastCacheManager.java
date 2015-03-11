package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.util.EmptyStatement;

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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.ValidationUtil.checkNotNull;

/**
 * Abstract {@link HazelcastCacheManager} (also {@link CacheManager} as indirect) implementation
 * provides shared functionality to server and client cache managers.
 * There are two cache managers which can be accessed via their providers.
 * <ul>
 *     <li>Client: HazelcastClientCacheManager.</li>
 *     <li>Server: HazelcastServerCacheManager.</li>
 * </ul>
 * <p>
 *    {@link AbstractHazelcastCacheManager} manages the lifecycle of the caches created or accessed through itself.
 * </p>
 * @see HazelcastCacheManager
 * @see CacheManager
 */
public abstract class AbstractHazelcastCacheManager
        implements HazelcastCacheManager {

    protected final ConcurrentMap<String, ICache<?, ?>> caches = new ConcurrentHashMap<String, ICache<?, ?>>();
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

    @Override
    public <K, V, C extends Configuration<K, V>> ICache<K, V> createCache(String cacheName, C configuration)
            throws IllegalArgumentException {
        checkIfManagerNotClosed();
        checkNotNull(cacheName, "cacheName must not be null");
        checkNotNull(configuration, "configuration must not be null");

        CacheConfig<K, V> newCacheConfig = createCacheConfig(cacheName, configuration);
        if (caches.containsKey(newCacheConfig.getNameWithPrefix())) {
            throw new CacheException("A cache named " + cacheName + " already exists.");
        }
        // Create cache config on all nodes as sync
        CacheConfig<K, V> currentCacheConfig = createConfig(cacheName, newCacheConfig, true, true);
        // Create cache proxy object with cache config
        ICache<K, V> cacheProxy = createCacheProxy(newCacheConfig);
        if (currentCacheConfig == null) {
            // Put created cache config.
            // Single thread region because "createConfigOnPartition" is single threaded by partition thread
            addCacheConfigIfAbsent(newCacheConfig);
            // Put created cache. No need to a "putIfAbsent" as this is a single threaded region
            caches.put(newCacheConfig.getNameWithPrefix(), cacheProxy);
            // Register listeners
            registerListeners(newCacheConfig, cacheProxy);
            return cacheProxy;
        }
        ICache<?, ?> cache = getOrPutIfAbsent(currentCacheConfig.getNameWithPrefix(), cacheProxy);
        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        if (config.equals(newCacheConfig)) {
            return (ICache<K, V>) cache;
        }
        throw new CacheException("A cache named " + cacheName + " already exists.");
    }

    private ICache<?, ?> getOrPutIfAbsent(String nameWithPrefix, ICache cacheProxy) {
        ICache<?, ?> cache = caches.get(nameWithPrefix);
        if (cache == null) {
            ICache<?, ?> iCache = caches.putIfAbsent(nameWithPrefix, cacheProxy);
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
        checkIfManagerNotClosed();
        checkNotNull(keyType, "keyType can not be null");
        checkNotNull(valueType, "valueType can not be null");
        ICache<?, ?> cache = getCacheUnchecked(cacheName);
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
        checkIfManagerNotClosed();
        ICache<?, ?> cache = getCacheUnchecked(cacheName);
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
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICache<?, ?> cache = caches.get(cacheNameWithPrefix);
        if (cache == null) {
            CacheConfig<K, V> cacheConfig = findConfig(cacheNameWithPrefix, cacheName, true, true);
            if (cacheConfig == null) {
                // No cache found
                return null;
            }
            // Create the cache proxy which already exists in the cluster as config
            ICache<K, V> cacheProxy = createCacheProxy(cacheConfig);
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
        Set<String> names;
        if (isClosed()) {
            names = Collections.emptySet();
        } else {
            names = new LinkedHashSet<String>();
            for (Map.Entry<String, ICache<?, ?>> entry : caches.entrySet()) {
                String nameWithPrefix = entry.getKey();
                int index = nameWithPrefix.indexOf(cacheNamePrefix) + cacheNamePrefix.length();
                final String name = nameWithPrefix.substring(index);
                names.add(name);
            }
        }
        return Collections.unmodifiableCollection(names);
    }

    @Override
    public void destroyCache(String cacheName) {
        checkIfManagerNotClosed();
        checkNotNull(cacheName, "cacheName cannot be null");
        String cacheNameWithPrefix = getCacheNameWithPrefix(cacheName);
        ICache<?, ?> cache = caches.remove(cacheNameWithPrefix);
        if (cache != null) {
            cache.destroy();
        }
        removeCacheConfigFromLocal(cacheNameWithPrefix);
    }

    /**
     * Removes the local copy of the cache configuration if one exists.
     * Default implementation does not require it. But client implementation overrides this to track a local copy
     * of the config.
     *
     * @param cacheName cache name.
     */
    protected void removeCacheConfigFromLocal(String cacheName) {
    }

    private String registerLifecycleListener() {
        return hazelcastInstance.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    close();
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
            EmptyStatement.ignore(e);
        }
    }

    @Override
    public void close() {
        if (isDestroyed.get() || !isClosed.compareAndSet(false, true)) {
            return;
        }

        deregisterLifecycleListener();
        for (ICache cache : caches.values()) {
            cache.close();
        }
        postClose();
        // TODO do we need to clear it as "caches.clear();"
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

    protected void checkIfManagerNotClosed() {
        if (isClosed()) {
            throw new IllegalStateException();
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
        StringBuilder sb = new StringBuilder("/hz");
        if (!isDefaultURI) {
            sb.append("/").append(uri.toASCIIString());
        }
        ClassLoader classLoader = getClassLoader();
        if (!isDefaultClassLoader && classLoader != null) {
            sb.append("/").append(classLoader.toString());
        }
        sb.append("/");
        return sb.toString();
    }

    protected String getCacheNameWithPrefix(String name) {
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
            iterator.remove();
            source.registerCacheEntryListener(listenerConfig);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HazelcastCacheManager{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append(", cachingProvider=").append(cachingProvider);
        sb.append('}');
        return sb.toString();
    }

    protected abstract <K, V> void addCacheConfigIfAbsent(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> ICache<K, V> createCacheProxy(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> CacheConfig<K, V> findConfig(String cacheName,
                                                           String simpleCacheName,
                                                           boolean createAlsoOnOthers,
                                                           boolean syncCreate);

    protected abstract <K, V> CacheConfig<K, V> createConfig(String cacheName,
                                                             CacheConfig<K, V> config,
                                                             boolean createAlsoOnOthers,
                                                             boolean syncCreate);

    protected abstract <K, V> CacheConfig<K, V> getCacheConfigLocal(String cacheName);

    protected abstract <K, V> CacheConfig<K, V> createConfigOnPartition(CacheConfig<K, V> cacheConfig);

    protected abstract <K, V> CacheConfig<K, V> getCacheConfigFromPartition(String cacheName,
                                                                            String simpleCacheName);

    protected abstract void postClose();

}
