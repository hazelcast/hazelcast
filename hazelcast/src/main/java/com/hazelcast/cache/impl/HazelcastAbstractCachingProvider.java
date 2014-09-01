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

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;
import java.util.WeakHashMap;


public abstract class HazelcastAbstractCachingProvider
        implements CachingProvider {

    protected static final ILogger logger = Logger.getLogger(HazelcastCachingProvider.class);
    protected static HazelcastInstance hazelcastInstance;

    private WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> cacheManagers;

    public HazelcastAbstractCachingProvider() {
        this.cacheManagers = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();
    }

    @Override
    public synchronized CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        URI managerURI = uri == null ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        Properties managerProperties = properties == null ? new Properties() : properties;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
        if (cacheManagersByURI == null) {
            cacheManagersByURI = new HashMap<URI, CacheManager>();
        }
        CacheManager cacheManager = cacheManagersByURI.get(managerURI);
        if (cacheManager == null) {
            try {
                cacheManager = getHazelcastCacheManager(uri, classLoader, managerProperties);
                cacheManagersByURI.put(managerURI, cacheManager);
            } catch (Throwable t) {
                logger.warning("Error opening URI" + managerURI.toString() + ". Caused by :" + t.getMessage());
            }
        }
        if (!cacheManagers.containsKey(managerClassLoader)) {
            cacheManagers.put(managerClassLoader, cacheManagersByURI);
        }
        return cacheManager;
    }

    protected HazelcastInstance getHazelcastInstance() {
        if (hazelcastInstance == null) {
            hazelcastInstance = initHazelcast();
        }
        return hazelcastInstance;
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        try {
            return new URI(this.getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create Default URI", e);
        }
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
    public synchronized void close() {
        WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> managersByClassLoader = this.cacheManagers;
        this.cacheManagers = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();

        for (ClassLoader classLoader : managersByClassLoader.keySet()) {
            for (CacheManager cacheManager : managersByClassLoader.get(classLoader).values()) {
                cacheManager.close();
            }
        }
        shutdownHazelcastInstance();
    }

    protected void shutdownHazelcastInstance() {
        if(hazelcastInstance != null){
            hazelcastInstance.shutdown();
        }
        hazelcastInstance = null;
    }

    @Override
    public synchronized void close(ClassLoader classLoader) {
        final ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        final HashMap<URI, CacheManager> cacheManagersByURI = this.cacheManagers.remove(managerClassLoader);
        if (cacheManagersByURI != null) {
            for (CacheManager cacheManager : cacheManagersByURI.values()) {
                cacheManager.close();
            }
        }
    }

    @Override
    public synchronized void close(URI uri, ClassLoader classLoader) {
        final HashMap<URI, CacheManager> uriCacheManagerHashMap = this.cacheManagers.get(classLoader);
        if (uriCacheManagerHashMap != null) {
            final CacheManager cacheManager = uriCacheManagerHashMap.get(uri);
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
        }
        return false;
    }

    public synchronized void releaseCacheManager(URI uri, ClassLoader classLoader) {
        final URI managerURI = uri == null ? getDefaultURI() : uri;
        final ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
        if (cacheManagersByURI != null) {
            cacheManagersByURI.remove(managerURI);

            if (cacheManagersByURI.size() == 0) {
                cacheManagers.remove(managerClassLoader);
            }
        }
    }

    protected abstract HazelcastInstance initHazelcast();

    protected abstract CacheManager getHazelcastCacheManager(URI uri, ClassLoader classLoader, Properties managerProperties);
}
