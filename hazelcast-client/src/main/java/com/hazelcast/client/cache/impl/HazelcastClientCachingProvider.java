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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.AbstractHazelcastCachingProvider;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * Provides client cachingProvider implementation.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastClientCachingProvider extends AbstractHazelcastCachingProvider {

    public HazelcastClientCachingProvider() {
    }

    /**
     * Helper method for creating caching provider for testing etc
     * @param hazelcastInstance
     * @return HazelcastClientCachingProvider
     */
    public static HazelcastClientCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        final HazelcastClientCachingProvider cachingProvider = new HazelcastClientCachingProvider();
        cachingProvider.hazelcastInstance = hazelcastInstance;
        return cachingProvider;
    }

    @Override
    protected HazelcastClientCacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader,
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
        return new HazelcastClientCacheManager(this, instance, uri, classLoader, properties);
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
        // If config location is specified, get instance through it.
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
                final ClientConfig config = new XmlClientConfigBuilder(configURL).build();
                config.setClassLoader(theClassLoader);
                return HazelcastClient.newHazelcastClient(config);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        // If config location is specified, get instance with its name.
        String instanceName = properties.getProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME);
        if (instanceName != null) {
            return HazelcastClient.getHazelcastClientByName(instanceName);
        }

        return getInstanceThroughDefaultInstanceIfItIsDefault(isDefaultURI);
    }

    private HazelcastInstance getInstanceThroughDefaultInstanceIfItIsDefault(boolean isDefaultURI) {
        HazelcastInstance instance = null;
        // No instance specified with name of config location,
        // so we are going on with the default one if the URI is the default.
        if (isDefaultURI) {
            if (hazelcastInstance == null) {
                // If there is no default instance in use (not created yet and not specified), create a new one.
                instance = HazelcastClient.newHazelcastClient();
                // Since there is no default instance in use, set new instance as default one.
                hazelcastInstance = instance;
            } else {
                // Use the existing default instance.
                instance = hazelcastInstance;
            }
        }
        return instance;
    }

    @Override
    public String toString() {
        return "HazelcastClientCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }
}
