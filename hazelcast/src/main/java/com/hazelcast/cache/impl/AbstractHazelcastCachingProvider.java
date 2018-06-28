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

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.StringUtil;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

/**
 * Abstract {@link CachingProvider} implementation providing shared
 * functionality to server and client caching providers.
 * <p>
 * This class encapsulates following details:
 * <ul>
 * <li>Hazelcast instance for the default URI and default classloader</li>
 * <li>the default URI</li>
 * <li>the default classloader</li>
 * <li>accessing the singleton {@link CacheManager} by URI and classloader</li>
 * <li>managing lifecycle of cache managers</li>
 * </ul>
 *
 * @see CachingProvider
 */
public abstract class AbstractHazelcastCachingProvider implements CachingProvider {

    /**
     * Name of default {@link HazelcastInstance} which may be started when
     * obtaining the default {@link CachingProvider}.
     */
    public static final String SHARED_JCACHE_INSTANCE_NAME = "_hzinstance_jcache_shared";

    /**
     * System property to control whether the default Hazelcast instance, which
     * may be started when obtaining the default {@link CachingProvider}, will
     * have an instance name set or not.
     * <p>
     * When not set or when system property {@code hazelcast.named.jcache.instance}
     * is {@code true}, then a common instance name is used to get-or-create
     * the default {@link HazelcastInstance}. When the system property
     * {@code hazelcast.named.jcache.instance} is {@code false}, then no
     * instance name is set on the default configuration.
     */
    public static final String NAMED_JCACHE_HZ_INSTANCE = "hazelcast.named.jcache.instance";

    protected static final ILogger LOGGER = Logger.getLogger(HazelcastCachingProvider.class);

    private static final String INVALID_HZ_INSTANCE_SPECIFICATION_MESSAGE = "No available Hazelcast instance."
            + " Please specify your Hazelcast configuration file path via"
            + " \"HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION\" property or specify Hazelcast instance name via"
            + " \"HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME\" property in the \"properties\" parameter.";
    private static final Set<String> SUPPORTED_SCHEMES;

    static {
        Set<String> supportedSchemes = new HashSet<String>();
        supportedSchemes.add("classpath");
        supportedSchemes.add("file");
        supportedSchemes.add("http");
        supportedSchemes.add("https");
        SUPPORTED_SCHEMES = supportedSchemes;
    }

    protected final boolean namedDefaultHzInstance = parseBoolean(getProperty(NAMED_JCACHE_HZ_INSTANCE, "true"));

    protected volatile HazelcastInstance hazelcastInstance;

    private final ClassLoader defaultClassLoader;
    private final URI defaultURI;

    private final Map<ClassLoader, Map<URI, AbstractHazelcastCacheManager>> cacheManagers;

    protected AbstractHazelcastCachingProvider() {
        // we use a WeakHashMap to prevent strong references to a classLoader to avoid memory leaks
        this.cacheManagers = new WeakHashMap<ClassLoader, Map<URI, AbstractHazelcastCacheManager>>();
        this.defaultClassLoader = getClass().getClassLoader();
        try {
            defaultURI = new URI("hazelcast");
        } catch (URISyntaxException e) {
            throw new CacheException("Cannot create default URI", e);
        }
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        URI managerURI = getManagerUri(uri);
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        Properties managerProperties = properties == null ? new Properties() : properties;
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
                    throw new CacheException("Error opening URI [" + managerURI.toString() + ']', e);
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
        // closing a CachingProvider does not mean to close it forever, see JavaDoc of close()
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

    private void shutdownHazelcastInstance() {
        HazelcastInstance localInstanceRef = hazelcastInstance;
        if (localInstanceRef != null) {
            localInstanceRef.shutdown();
        }
        hazelcastInstance = null;
    }

    @Override
    public void close(ClassLoader classLoader) {
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                for (CacheManager cacheManager : cacheManagersByURI.values()) {
                    cacheManager.close();
                }
            }
        }
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        URI managerURI = getManagerUri(uri);
        ClassLoader managerClassLoader = getManagerClassLoader(classLoader);
        synchronized (cacheManagers) {
            Map<URI, AbstractHazelcastCacheManager> cacheManagersByURI = this.cacheManagers.get(managerClassLoader);
            if (cacheManagersByURI != null) {
                CacheManager cacheManager = cacheManagersByURI.remove(managerURI);
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
            // Hazelcast is distributed only and does not have a local in-process mode,
            // therefore the optional store-by-reference mode is not supported
            case STORE_BY_REFERENCE:
                return false;
            default:
                return false;
        }
    }

    private URI getManagerUri(URI uri) {
        return uri == null ? defaultURI : uri;
    }

    private ClassLoader getManagerClassLoader(ClassLoader classLoader) {
        return classLoader == null ? defaultClassLoader : classLoader;
    }

    private <T extends AbstractHazelcastCacheManager> T createHazelcastCacheManager(URI uri, ClassLoader classLoader,
                                                                                    Properties managerProperties) {
        HazelcastInstance instance;
        try {
            instance = getOrCreateInstance(uri, classLoader, managerProperties);
            if (instance == null) {
                throw new IllegalArgumentException(INVALID_HZ_INSTANCE_SPECIFICATION_MESSAGE);
            }
        } catch (Exception e) {
            throw rethrow(e);
        }
        return createCacheManager(instance, uri, classLoader, managerProperties);
    }

    protected abstract HazelcastInstance getOrCreateInstance(URI uri, ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException;

    protected abstract <T extends AbstractHazelcastCacheManager> T createCacheManager(HazelcastInstance instance,
                                                                                      URI uri, ClassLoader classLoader,
                                                                                      Properties properties);
    // returns true when location itself or its resolved value as system property placeholder has one of supported schemes
    // from which Config objects can be initialized
    protected boolean isConfigLocation(URI location) {
        String scheme = location.getScheme();
        if (scheme == null) {
            // interpret as place holder
            try {
                String resolvedPlaceholder = getProperty(location.getRawSchemeSpecificPart());
                if (resolvedPlaceholder == null) {
                    return false;
                }
                location = new URI(resolvedPlaceholder);
                scheme = location.getScheme();
            } catch (URISyntaxException e) {
                return false;
            }
        }
        return (scheme != null && SUPPORTED_SCHEMES.contains(scheme.toLowerCase(StringUtil.LOCALE_INTERNAL)));
    }
}
