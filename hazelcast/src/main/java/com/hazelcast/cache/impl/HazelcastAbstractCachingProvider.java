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

package com.hazelcast.cache.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class HazelcastAbstractCachingProvider
        implements CachingProvider {

    protected static final ILogger LOGGER = Logger.getLogger(HazelcastCachingProvider.class);
    protected static volatile HazelcastInstance hazelcastInstance;

    protected final ClassLoader defaultClassLoader;
    protected final URI defaultURI;

    private final ConcurrentMap<ClassLoader, ConcurrentMap<URI, CacheManager>> cacheManagers;

    public HazelcastAbstractCachingProvider() {
        //we use a concurrent weak HashMap to prevent strong references to a classLoader to avoid memory leak.
        this.cacheManagers = new ConcurrentReferenceHashMap<ClassLoader, ConcurrentMap<URI, CacheManager>>();
        this.defaultClassLoader = this.getClass().getClassLoader();
        try {
            defaultURI = new URI(this.getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create Default URI", e);
        }
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        final URI managerURI = getManagerUri(uri);
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        final Properties managerProperties = properties == null ? new Properties() : properties;

        if (cacheManagers.get(managerClassLoader) == null) {
            cacheManagers.putIfAbsent(managerClassLoader, new ConcurrentHashMap<URI, CacheManager>());
        }
        final ConcurrentMap<URI, CacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
        CacheManager cacheManager = cacheManagersByURI.get(managerURI);
        if (cacheManager == null) {
            try {
                cacheManager = createHazelcastCacheManager(uri, classLoader, managerProperties);
                final CacheManager oldCacheManager = cacheManagersByURI.putIfAbsent(managerURI, cacheManager);
                if (oldCacheManager != null) {
                    cacheManager = oldCacheManager;
                }
            } catch (Exception e) {
                throw new CacheException("Error opening URI" + managerURI.toString(), e);
            }
        }
        return cacheManager;
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return defaultClassLoader;
    }

    @Override
    public URI getDefaultURI() {
        return defaultURI;
    }

    @Override
    public Properties getDefaultProperties() {
        return null;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, null);
    }

    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(null, null, null);
    }

    @Override
    public void close() {
        //closing a cacheProvider do not close it forever see javadoc of close()
        for (ClassLoader classLoader : cacheManagers.keySet()) {
            close(classLoader);
        }
        this.cacheManagers.clear();
        shutdownHazelcastInstance();
    }

    protected void shutdownHazelcastInstance() {
        final HazelcastInstance localInstanceRef = hazelcastInstance;
        if (localInstanceRef != null) {
            //shutdown is thread safe
            hazelcastInstance.shutdown();
        }
        hazelcastInstance = null;
    }

    @Override
    public void close(ClassLoader classLoader) {
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        final ConcurrentMap<URI, CacheManager> cacheManagersByURI = this.cacheManagers.remove(managerClassLoader);
        if (cacheManagersByURI != null) {
            for (CacheManager cacheManager : cacheManagersByURI.values()) {
                cacheManager.close();
            }
        }
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        final URI managerURI = getManagerUri(uri);
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);

        final ConcurrentMap<URI, CacheManager> uriCacheManagerMap = this.cacheManagers.get(managerClassLoader);
        if (uriCacheManagerMap != null) {
            final CacheManager cacheManager = uriCacheManagerMap.get(managerURI);
            if (cacheManager != null) {
                cacheManager.close();
            }
        }
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        switch (optionalFeature) {
            case STORE_BY_REFERENCE:
                return false;
            default:
                return false;
        }
    }

    public void releaseCacheManager(URI uri, ClassLoader classLoader) {
        final URI managerURI = getManagerUri(uri);
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);

        ConcurrentMap<URI, CacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
        if (cacheManagersByURI != null) {
            cacheManagersByURI.remove(managerURI);
            //cacheManagersByURI has weak referenced key so no need to remove it, if has size zero
        }
    }

    protected URI getManagerUri(URI uri) {
        return uri == null ? defaultURI : uri;
    }

    protected ClassLoader getManagerClassLoader(ClassLoader classLoader) {
        return classLoader == null ? defaultClassLoader : classLoader;
    }

    protected abstract CacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader, Properties managerProperties);
}
