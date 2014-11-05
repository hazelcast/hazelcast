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

import com.hazelcast.cache.HazelcastCachingProvider;
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
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

/**
 * Abstract {@link CachingProvider} implementation providing shared functionality to server and client caching
 * providers.
 * <p>This class encapsulates following details:
 * <ul>
 *     <li>Hazelcast instance for default uri and default class loader.</li>
 *     <li>default uri.</li>
 *     <li>default class loader.</li>
 *     <li>accessing the singleton {@link CacheManager} by uri and class loader.</li>
 *     <li>managing lifecycle of cache managers.</li>
 * </ul></p>
 * @see CachingProvider
 */
public abstract class AbstractHazelcastCachingProvider
        implements CachingProvider {

    protected static final ILogger LOGGER = Logger.getLogger(HazelcastCachingProvider.class);
    protected volatile HazelcastInstance hazelcastInstance;

    protected final ClassLoader defaultClassLoader;
    protected final URI defaultURI;

    private final Map<ClassLoader, Map<URI, AbstractHazelcastCacheManager>> cacheManagers;

    public AbstractHazelcastCachingProvider() {
        //we use a WeakHashMap to prevent strong references to a classLoader to avoid memory leak.
        this.cacheManagers = new WeakHashMap<ClassLoader, Map<URI, AbstractHazelcastCacheManager>>();
        this.defaultClassLoader = this.getClass().getClassLoader();
        try {
            defaultURI = new URI("hazelcast");
            //            defaultURI = new URI(this.getClass().getName());
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create Default URI", e);
        }
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        final URI managerURI = getManagerUri(uri);
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        final Properties managerProperties = properties == null ? new Properties() : properties;
        synchronized (cacheManagers) {
            Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI = cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI == null) {
                cacheManagersByURI = new HashMap<URI, AbstractHazelcastCacheManager>();
                cacheManagers.put(managerClassLoader, cacheManagersByURI);
            }
            AbstractHazelcastCacheManager cacheManager = cacheManagersByURI.get(managerURI);
            if (cacheManager == null || cacheManager.isClosed()) {
                try {
                    cacheManager = createHazelcastCacheManager(uri, classLoader, managerProperties);
                    cacheManagersByURI.put(managerURI, cacheManager);
                } catch (Exception e) {
                    throw new CacheException("Error opening URI " + managerURI.toString(), e);
                }
            }
            return cacheManager;
        }
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
        synchronized (cacheManagers) {
            for (Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI : cacheManagers.values()) {
                for (AbstractHazelcastCacheManager cacheManager : cacheManagersByURI.values()) {
                    if (cacheManager.isDefaultClassLoader) {
                        cacheManager.close();
                    } else {
                        cacheManager.destroy();
                    }
                }
            }
        }
        this.cacheManagers.clear();
        shutdownHazelcastInstance();
    }

    protected void shutdownHazelcastInstance() {
        final HazelcastInstance localInstanceRef = hazelcastInstance;
        if (localInstanceRef != null) {
            localInstanceRef.shutdown();
        }
        hazelcastInstance = null;
    }

    @Override
    public void close(ClassLoader classLoader) {
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            final Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                for (CacheManager cacheManager : cacheManagersByURI.values()) {
                    cacheManager.close();
                }
            }
        }
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        final URI managerURI = getManagerUri(uri);
        final ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            final Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                final CacheManager cacheManager = cacheManagersByURI.remove(managerURI);
                if (cacheManager != null) {
                    cacheManager.close();
                }
                if (cacheManagersByURI.isEmpty()) {
                    cacheManagers.remove(classLoader);
                }
            }
        }
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        switch (optionalFeature) {
            // Hazelcast is distributed only and does not have a local in-process mode. Therefore the optional
            // store-by-reference mode is not supported.
            case STORE_BY_REFERENCE:
                return false;
            default:
                return false;
        }
    }

    protected URI getManagerUri(URI uri) {
        return uri == null ? defaultURI : uri;
    }

    protected ClassLoader getManagerClassLoader(ClassLoader classLoader) {
        return classLoader == null ? defaultClassLoader : classLoader;
    }

    protected abstract <T extends AbstractHazelcastCacheManager> T createHazelcastCacheManager(URI uri, ClassLoader classLoader,
                                                                         Properties managerProperties);
}
