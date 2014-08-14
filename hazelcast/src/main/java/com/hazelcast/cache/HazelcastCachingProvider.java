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

package com.hazelcast.cache;


import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Hazelcast implementation of {@link CachingProvider}
 *
 * @author asimarslan
 */
public class HazelcastCachingProvider implements CachingProvider {

    private final static ILogger logger = Logger.getLogger(HazelcastCachingProvider.class);
    private static HazelcastInstance hazelcastInstance;

    private WeakHashMap<ClassLoader, HashMap<URI, CacheManager>> cacheManagers;

    public HazelcastCachingProvider() {
        this.cacheManagers = new WeakHashMap<ClassLoader, HashMap<URI, CacheManager>>();
        if(hazelcastInstance == null){
            Config config = new XmlConfigBuilder().build();
            hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        }
    }

    /**
     * {@inheritDoc}
     */
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
                cacheManager = new HazelcastCacheManager(this, hazelcastInstance, managerURI, managerClassLoader, managerProperties);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public ClassLoader getDefaultClassLoader() {
        return getClass().getClassLoader();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI getDefaultURI() {
        try {
            return new URI(this.getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create Default URI", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Properties getDefaultProperties() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, getDefaultProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader(), null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        final Set<ClassLoader> classLoaderSet = new HashSet<ClassLoader>(this.cacheManagers.keySet());
        for (ClassLoader classLoader : classLoaderSet) {
            close(classLoader);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close(ClassLoader classLoader) {
        final HashMap<URI, CacheManager> uriCacheManagerHashMap = this.cacheManagers.get(classLoader);
        if (uriCacheManagerHashMap != null) {
            for (CacheManager cacheManager : uriCacheManagerHashMap.values()) {
                cacheManager.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        switch (optionalFeature) {
            case STORE_BY_REFERENCE:
                return false;
        }
        return false;
    }

    public synchronized void releaseCacheManager(URI uri, ClassLoader classLoader) {
        URI managerURI = uri == null ? getDefaultURI() : uri;
        ClassLoader managerClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;

        HashMap<URI, CacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
        if (cacheManagersByURI != null) {
            cacheManagersByURI.remove(managerURI);

            if (cacheManagersByURI.size() == 0) {
                cacheManagers.remove(managerClassLoader);
            }
        }
    }
}
