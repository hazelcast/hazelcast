/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

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
    protected HazelcastServerCacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader,
                                                                      Properties properties) {
        final boolean isDefaultURI = (uri == null || uri.equals(getDefaultURI()));
        final HazelcastInstance instance;
        try {
            instance = getOrCreateInstance(classLoader, properties, isDefaultURI);
            if (instance == null) {
                throw new IllegalArgumentException(INVALID_HZ_INSTANCE_SPECIFICATION_MESSAGE);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new HazelcastServerCacheManager(this, instance, uri, classLoader, properties);
    }

    private HazelcastInstance getOrCreateInstance(ClassLoader classLoader, Properties properties, boolean isDefaultURI)
            throws URISyntaxException, IOException {
        HazelcastInstance instanceItself =
                (HazelcastInstance) properties.get(HazelcastCachingProvider.HAZELCAST_INSTANCE_ITSELF);

        // If instance itself is specified via properties, get instance through it.
        if (instanceItself != null) {
            return instanceItself;
        }

        String location = properties.getProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION);
        String instanceName = properties.getProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME);

        // If config location is specified via properties, get instance through it.
        if (location != null) {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            if (scheme == null) {
                // It is a place holder
                uri = new URI(System.getProperty(uri.getRawSchemeSpecificPart()));
            }
            ClassLoader theClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
            final URL configURL;
            if ("classpath".equals(scheme)) {
                configURL = theClassLoader.getResource(uri.getRawSchemeSpecificPart());
            } else if ("file".equals(scheme) || "http".equals(scheme) || "https".equals(scheme)) {
                configURL = uri.toURL();
            } else {
                throw new URISyntaxException(location, "Unsupported protocol in configuration location URL");
            }
            try {
                Config config = getConfig(configURL, theClassLoader, instanceName);
                return HazelcastInstanceManager.getOrCreateHazelcastInstance(config);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        // If instance name is specified via properties, get instance through it.
        if (instanceName != null) {
            return Hazelcast.getHazelcastInstanceByName(instanceName);
        }

        return getInstanceThroughDefaultInstanceIfItIsDefault(isDefaultURI);
    }

    private HazelcastInstance getInstanceThroughDefaultInstanceIfItIsDefault(boolean isDefaultURI) {
        HazelcastInstance instance = null;
        // No instance specified with name or config location,
        // so we are going on with the default one if the URI is the default.
        if (isDefaultURI) {
            if (hazelcastInstance == null) {
                // If there is no default instance in use (not created yet and not specified), create a new one.
                instance = Hazelcast.newHazelcastInstance();
                // Since there is no default instance in use, set new instance as default one.
                hazelcastInstance = instance;
            } else {
                // Use the existing default instance.
                instance = hazelcastInstance;
            }
        }
        return instance;
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
