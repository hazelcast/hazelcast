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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.properties.GroupProperty;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

/**
 * This class is the Hazelcast JCache public {@link javax.cache.spi.CachingProvider} implementation. This provider
 * class acts as the commonly used entry point for the JCache SPI to register Hazelcast JCache as an eligible JCache
 * implementation.
 * <p>Main purpose of this provider implementation is to delegate to the user-selected internal
 * {@link javax.cache.spi.CachingProvider} implementation.</p>
 * <p>Hazelcast uses two internal {@link javax.cache.spi.CachingProvider}s depending on the environment:
 * <ul>
 * <li>{@link com.hazelcast.cache.impl.HazelcastServerCachingProvider}</li>
 * <li>{@link com.hazelcast.client.cache.impl.HazelcastClientCachingProvider}</li>
 * </ul>
 * </p>
 * <p>
 * <h3>Provider Type Selection:</h3>
 * The first step is to check whether a selection exists using the system property
 * <tt>hazelcast.jcache.provider.type</tt> with values of <tt>client</tt> or <tt>server</tt>.
 * If no selection exists, then the default behavior for selecting the internal provider type is based on
 * which dependency is found on the classpath. Client and server provider classes are searched on the classpath. If
 * both {@link javax.cache.spi.CachingProvider} implementations are found (client and server), the client
 * provider has precedence. To select the server provider, use the above mentioned property.
 * </p>
 *
 * @since 3.4
 */
public final class HazelcastCachingProvider
        implements CachingProvider {

    /**
     * Hazelcast config location property
     */
    public static final String HAZELCAST_CONFIG_LOCATION = "hazelcast.config.location";

    /**
     * Hazelcast instance name property
     */
    public static final String HAZELCAST_INSTANCE_NAME = "hazelcast.instance.name";

    /**
     * Hazelcast instance itself property
     */
    public static final String HAZELCAST_INSTANCE_ITSELF = "hazelcast.instance.itself";

    private static final String CLIENT_CACHING_PROVIDER_CLASS = "com.hazelcast.client.cache.impl.HazelcastClientCachingProvider";
    private static final ILogger LOGGER = Logger.getLogger(HazelcastCachingProvider.class);

    private final CachingProvider delegate;

    public HazelcastCachingProvider() {
        CachingProvider cp = null;
        String providerType = GroupProperty.JCACHE_PROVIDER_TYPE.getSystemProperty();
        if (providerType != null) {
            if ("client".equals(providerType)) {
                cp = createClientProvider();
            }
            if ("server".equals(providerType)) {
                cp = new HazelcastServerCachingProvider();
            }
            if (cp == null) {
                throw new CacheException("CacheProvider cannot be created with the provided type: " + providerType);
            }
        } else {
            cp = createClientProvider();
            if (cp == null) {
                cp = new HazelcastServerCachingProvider();
            }
        }
        delegate = cp;
    }

    private CachingProvider createClientProvider() {
        try {
            return ClassLoaderUtil.newInstance(getClass().getClassLoader(), CLIENT_CACHING_PROVIDER_CLASS);
        } catch (Exception e) {
            LOGGER.finest("Could not load client CachingProvider! Fallback to server one... " + e.toString());
        }
        return null;
    }

    /**
     * Create the {@link java.util.Properties} with the provided config file location.
     *
     * @param configFileLocation the location of the config file to configure
     * @return properties instance pre-configured with the configuration location
     */
    public static Properties propertiesByLocation(String configFileLocation) {
        final Properties properties = new Properties();
        properties.setProperty(HAZELCAST_CONFIG_LOCATION, configFileLocation);
        return properties;
    }

    /**
     * Create the {@link java.util.Properties} with the provided instance name.
     *
     * @param instanceName the instance name to configure
     * @return the properties instance pre-configured with the instance name
     */
    public static Properties propertiesByInstanceName(String instanceName) {
        final Properties properties = new Properties();
        properties.setProperty(HAZELCAST_INSTANCE_NAME, instanceName);
        return properties;
    }

    /**
     * Create the {@link java.util.Properties} with the provided instance itself.
     *
     * @param instance the instance itself to be used
     * @return the properties instance pre-configured with the instance itself
     */
    public static Properties propertiesByInstanceItself(HazelcastInstance instance) {
        final Properties properties = new Properties();
        properties.put(HAZELCAST_INSTANCE_ITSELF, instance);
        return properties;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return delegate.getCacheManager(uri, classLoader, properties);
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return delegate.getDefaultClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return delegate.getDefaultURI();
    }

    @Override
    public Properties getDefaultProperties() {
        return delegate.getDefaultProperties();
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return delegate.getCacheManager(uri, classLoader);
    }

    @Override
    public CacheManager getCacheManager() {
        return delegate.getCacheManager();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(ClassLoader classLoader) {
        delegate.close(classLoader);
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        delegate.close(uri, classLoader);
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return delegate.isSupported(optionalFeature);
    }

    @Override
    public String toString() {
        return "HazelcastCachingProvider{delegate=" + delegate + '}';
    }
}
