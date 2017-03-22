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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractHazelcastCachingProvider;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_ITSELF;
import static com.hazelcast.cache.HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Client side {@link javax.cache.spi.CachingProvider} implementation.
 */
public final class HazelcastClientCachingProvider extends AbstractHazelcastCachingProvider {

    public HazelcastClientCachingProvider() {
    }

    /**
     * Helper method for creating caching provider for testing etc.
     */
    public static HazelcastClientCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        final HazelcastClientCachingProvider cachingProvider = new HazelcastClientCachingProvider();
        cachingProvider.hazelcastInstance = hazelcastInstance;
        return cachingProvider;
    }

    @Override
    protected HazelcastClientCacheManager createCacheManager(HazelcastInstance instance,
                                                             URI uri, ClassLoader classLoader,
                                                             Properties properties) {
        return new HazelcastClientCacheManager(this, instance, uri, classLoader, properties);
    }

    @Override
    protected HazelcastInstance getOrCreateInstance(URI uri, ClassLoader classLoader, Properties properties)
            throws URISyntaxException, IOException {
        HazelcastInstance instanceItself = (HazelcastInstance) properties.get(HAZELCAST_INSTANCE_ITSELF);

        // if instance itself is specified via properties, get instance through it
        if (instanceItself != null) {
            return instanceItself;
        }

        String location = properties.getProperty(HAZELCAST_CONFIG_LOCATION);
        String instanceName = properties.getProperty(HAZELCAST_INSTANCE_NAME);
        // if config location is specified, get instance through it
        if (location != null) {
            ClientConfig config = getConfigFromLocation(location, classLoader, instanceName);
            return getOrCreateInstanceByConfig(config);
        }

        // If config location is specified, get instance with its name.
        if (instanceName != null) {
            return HazelcastClient.getHazelcastClientByName(instanceName);
        }

        final boolean isDefaultURI = (uri == null || uri.equals(getDefaultURI()));
        if (!isDefaultURI) {
            try {
                // try locating a Hazelcast config at CacheManager URI
                ClientConfig config = getConfigFromLocation(uri, classLoader, null);
                return getOrCreateInstanceByConfig(config);
            } catch (Exception e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Could not get or create hazelcast instance from URI " + uri.toString(), e);
                }
            }

            try {
                // try again, this time interpreting CacheManager URI as hazelcast instance name
                return HazelcastClient.getHazelcastClientByName(uri.toString());
            } catch (Exception e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Could not get hazelcast instance from instance name " + uri.toString(), e);
                }
            }
            // could not locate hazelcast instance, return null and an exception will be thrown from invoker
            return null;
        } else {
            return getDefaultInstance();
        }
    }

    private HazelcastInstance getDefaultInstance() {
        if (hazelcastInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default ClientConfig: if it specifies an instance name, get-or-create an instance by that name
            // 2. otherwise start a new hazelcast client
            ClientConfig clientConfig = new XmlClientConfigBuilder().build();
            if (isNullOrEmptyAfterTrim(clientConfig.getInstanceName())) {
                hazelcastInstance = HazelcastClient.newHazelcastClient();
            } else {
                hazelcastInstance = getOrCreateInstanceByConfig(clientConfig);
            }
        }
        return hazelcastInstance;
    }

    protected ClientConfig getConfigFromLocation(String location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        URI uri = new URI(location);
        return getConfigFromLocation(uri, classLoader, instanceName);
    }

    protected ClientConfig getConfigFromLocation(URI uri, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        String scheme = uri.getScheme();
        if (scheme == null) {
            // it is a place holder
            uri = new URI(System.getProperty(uri.getRawSchemeSpecificPart()));
        }
        ClassLoader theClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL;
        if ("classpath".equals(scheme)) {
            configURL = theClassLoader.getResource(uri.getRawSchemeSpecificPart());
        } else if ("file".equals(scheme) || "http".equals(scheme) || "https".equals(scheme)) {
            configURL = uri.toURL();
        } else {
            throw new URISyntaxException(uri.toString(), "Unsupported protocol in configuration location URL");
        }
        try {
            return getConfig(configURL, classLoader, instanceName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientConfig getConfig(URL configURL, ClassLoader theClassLoader, String instanceName)
            throws IOException {
        ClientConfig config = new XmlClientConfigBuilder(configURL).build();
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

    // lookup by config.getInstanceName, if not found return a new HazelcastInstance
    private HazelcastInstance getOrCreateInstanceByConfig(ClientConfig config) {
        HazelcastInstance instance = HazelcastClient.getHazelcastClientByName(config.getInstanceName());
        if (instance == null) {
            instance = HazelcastClient.newHazelcastClient(config);
        }
        return instance;
    }

    @Override
    public String toString() {
        return "HazelcastClientCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }
}
