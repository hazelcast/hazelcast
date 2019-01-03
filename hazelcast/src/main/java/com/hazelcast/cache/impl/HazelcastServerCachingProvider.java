/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_ITSELF;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Hazelcast server implementation of {@link javax.cache.spi.CachingProvider}.
 * <p>
 * Used internally by {@link com.hazelcast.cache.HazelcastCachingProvider} when
 * the JCache type is configured as {@code server}.
 * <p>
 * This implementation creates a new singleton {@link HazelcastInstance}
 * member. This instance is provided into the created managers.
 * <p>
 * If you need to use your already created HazelcastInstance, you can directly
 * create a provider using
 * {@link #createCachingProvider(com.hazelcast.core.HazelcastInstance)}.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastServerCachingProvider extends AbstractHazelcastCachingProvider {

    /**
     * Helper method for creating caching provider for testing, etc.
     */
    public static HazelcastServerCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        HazelcastServerCachingProvider cachingProvider = new HazelcastServerCachingProvider();
        cachingProvider.hazelcastInstance = hazelcastInstance;
        return cachingProvider;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends AbstractHazelcastCacheManager> T createCacheManager(HazelcastInstance instance, URI uri,
                                                                             ClassLoader classLoader, Properties properties) {
        return (T) new HazelcastServerCacheManager(this, instance, uri, classLoader, properties);
    }

    @Override
    protected HazelcastInstance getOrCreateInstance(URI uri, ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException {
        // if the Hazelcast instance itself is specified via properties, return it
        HazelcastInstance instanceItself = (HazelcastInstance) properties.get(HAZELCAST_INSTANCE_ITSELF);
        if (instanceItself != null) {
            return instanceItself;
        }

        // if the config location is specified, get the Hazelcast instance through it
        String location = properties.getProperty(HAZELCAST_CONFIG_LOCATION);
        String instanceName = properties.getProperty(HAZELCAST_INSTANCE_NAME);
        if (location != null) {
            Config config = getConfigFromLocation(location, classLoader, instanceName);
            return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
        }

        // if instance name is specified, get the Hazelcast instance through it
        if (instanceName != null) {
            return getOrCreateByInstanceName(instanceName);
        }

        // resolving HazelcastInstance via properties failed, try with URI as XML configuration file location
        boolean isDefaultURI = (uri == null || uri.equals(getDefaultURI()));
        if (!isDefaultURI) {
            // attempt to resolve URI as config location or as instance name
            if (isConfigLocation(uri)) {
                try {
                    // try locating a Hazelcast config at CacheManager URI
                    Config config = getConfigFromLocation(uri, classLoader, null);
                    return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
                } catch (Exception e) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Could not get or create Hazelcast instance from URI " + uri.toString(), e);
                    }
                }
            } else {
                try {
                    // try again, this time interpreting CacheManager URI as Hazelcast instance name
                    return getOrCreateByInstanceName(uri.toString());
                } catch (Exception e) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Could not get Hazelcast instance from instance name " + uri.toString(), e);
                    }
                }
            }
            // could not locate the Hazelcast instance, return null and an exception will be thrown by the invoker
            return null;
        } else {
            return getDefaultInstance();
        }
    }

    private HazelcastInstance getDefaultInstance() {
        if (hazelcastInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default ClientConfig: if it specifies an instance name, get-or-create an instance by that name
            // 2. otherwise start a new Hazelcast member
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
     * Gets an existing {@link HazelcastInstance} by {@code instanceName} or,
     * if not found, creates a new {@link HazelcastInstance} with the default
     * configuration and given {@code instanceName}.
     *
     * @param instanceName name to lookup an existing {@link HazelcastInstance}
     *                     or to create a new one
     * @return a {@link HazelcastInstance} with the given {@code instanceName}
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
        Config config = new XmlConfigBuilder().build();
        if (namedDefaultHzInstance && isNullOrEmpty(config.getInstanceName())) {
            config.setInstanceName(SHARED_JCACHE_INSTANCE_NAME);
        }
        return config;
    }

    private Config getConfigFromLocation(String location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        URI uri = new URI(location);
        return getConfigFromLocation(uri, classLoader, instanceName);
    }

    private Config getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        String scheme = location.getScheme();
        if (scheme == null) {
            // interpret as place holder
            location = new URI(System.getProperty(location.getRawSchemeSpecificPart()));
            scheme = location.getScheme();
        }
        ClassLoader theClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL;
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
            throw rethrow(e);
        }
    }

    private Config getConfig(URL configURL, ClassLoader theClassLoader, String instanceName) throws IOException {
        Config config = new XmlConfigBuilder(configURL).build()
                .setClassLoader(theClassLoader);
        if (instanceName != null) {
            // if the instance name is specified via properties use it,
            // even though instance name is specified in the config
            config.setInstanceName(instanceName);
        } else if (config.getInstanceName() == null) {
            // use the config URL as instance name if instance name is not specified
            config.setInstanceName(configURL.toString());
        }
        return config;
    }

    @Override
    public String toString() {
        return "HazelcastServerCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }
}
