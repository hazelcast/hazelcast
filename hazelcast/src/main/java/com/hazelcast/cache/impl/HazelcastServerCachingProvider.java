/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_ITSELF;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Provides server cachingProvider implementation. <p>This implementation is used by {@link
 * com.hazelcast.cache.HazelcastCachingProvider} internally when server type is configured.</p> <p>This implementation creates a
 * new singleton hazelcastInstance node. This instance is provided into the created managers.</p> <p>If you need to use your
 * already created HazelcastInstance, you can directly create a provider using
 * {@link #createCachingProvider(com.hazelcast.core.HazelcastInstance)}.</p>
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastServerCachingProvider
        extends AbstractHazelcastCachingProvider {

    /**
     * Helper method for creating caching provider for testing, etc.
     *
     * @param hazelcastInstance
     *
     * @return HazelcastServerCachingProvider
     */
    public static HazelcastServerCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        final HazelcastServerCachingProvider cachingProvider = new HazelcastServerCachingProvider();
        cachingProvider.hazelcastInstance = hazelcastInstance;
        return cachingProvider;
    }

    @Override
    protected HazelcastServerCacheManager createCacheManager(HazelcastInstance instance,
                                                             URI uri, ClassLoader classLoader,
                                                             Properties properties) {
        return new HazelcastServerCacheManager(this, instance, uri, classLoader, properties);
    }

    @Override
    protected HazelcastInstance getOrCreateInstance(URI uri, ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException {
        HazelcastInstance instanceItself = (HazelcastInstance) properties.get(HAZELCAST_INSTANCE_ITSELF);

        // If instance itself is specified via properties, get instance through it.
        if (instanceItself != null) {
            return instanceItself;
        }

        String location = properties.getProperty(HAZELCAST_CONFIG_LOCATION);
        String instanceName = properties.getProperty(HAZELCAST_INSTANCE_NAME);

        // If config location is specified via properties, get instance through it.
        if (location != null) {
            Config config = getConfigFromLocation(location, classLoader, instanceName);
            return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
        }

        // If instance name is specified via properties, get instance through it or create a new instance with default config
        // and given instance name
        if (instanceName != null) {
            HazelcastInstance instance = getOrCreateByInstanceName(instanceName);
            return instance;
        }

        // resolving HazelcastInstance via properties failed, try with URI as XML configuration file location
        final boolean isDefaultURI = (uri == null || uri.equals(getDefaultURI()));
        if (!isDefaultURI) {
            // attempt to resolve URI as config location or as instance name
            if (isConfigLocation(uri)) {
                try {
                    // try locating a Hazelcast config at CacheManager URI
                    Config config = getConfigFromLocation(uri, classLoader, null);
                    return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
                } catch (Exception e) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Could not get or create hazelcast instance from URI " + uri.toString(), e);
                    }
                }
            } else {
                try {
                    // try again, this time interpreting CacheManager URI as hazelcast instance name
                    return getOrCreateByInstanceName(uri.toString());
                } catch (Exception e) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Could not get hazelcast instance from instance name" + uri.toString(), e);
                    }
                }
            }
            // could not locate hazelcast instance, return null and an exception will be thrown from invoker
            return null;
        } else {
            return getDefaultInstance();
        }
    }

    protected HazelcastInstance getDefaultInstance() {
        if (hazelcastInstance == null) {
            // Since there is no default instance in use, get-or-create by instance name in default config or create new
            Config config = getDefaultConfig();
            if (isNullOrEmptyAfterTrim(config.getInstanceName())) {
                hazelcastInstance = Hazelcast.newHazelcastInstance();
            } else {
                hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
            }
        }
        return hazelcastInstance;
    }

    /**
     * Get an existing {@link HazelcastInstance} by {@code instanceName} or, if not found, create a new {@link HazelcastInstance}
     * with default configuration and given {@code instanceName}.
     *
     * @param instanceName name by which to lookup existing {@link HazelcastInstance} or create new one.
     * @return             a {@link HazelcastInstance} with the given {@code instanceName}
     */
    private HazelcastInstance getOrCreateByInstanceName(String instanceName) {
        HazelcastInstance instance = Hazelcast.getHazelcastInstanceByName(instanceName);
        if (instance == null) {
            Config config = getDefaultConfig();
            config.setInstanceName(instanceName);
            instance = Hazelcast.getOrCreateHazelcastInstance(config);
        }
        return instance;
    }

    private Config getDefaultConfig() {
        return new XmlConfigBuilder().build();
    }

    private Config getConfigFromLocation(String location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        URI configUri = new URI(location);
        return getConfigFromLocation(configUri, classLoader, instanceName);
    }

    private Config getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            // It may be a place holder
            location = new URI(System.getProperty(location.getRawSchemeSpecificPart()));
            scheme = location.getScheme();
        }
        ClassLoader theClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        final URL configURL;
        if ("classpath".equals(scheme)) {
            configURL = theClassLoader.getResource(location.getRawSchemeSpecificPart());
        } else if ("file".equals(scheme) || "http".equals(scheme) || "https".equals(scheme)) {
            configURL = location.toURL();
        } else {
            throw new URISyntaxException(location.toString(), "Unsupported protocol in configuration location URL");
        }
        try {
            return getConfig(configURL, theClassLoader, instanceName);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Config getConfig(URL configURL, ClassLoader theClassLoader, String instanceName)
            throws IOException {
        Config config = new XmlConfigBuilder(configURL).build();
        config.setClassLoader(theClassLoader);
        if (instanceName != null) {
            // If instance name is specified via properties use it
            // even though instance name is specified in the config.
            config.setInstanceName(instanceName);
        } else if (config.getInstanceName() == null) {
            // Use config url as instance name if instance name is not specified.
            config.setInstanceName(configURL.toString());
        }
        return config;
    }

    @Override
    public String toString() {
        return "HazelcastServerCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }
}
