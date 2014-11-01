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
        super();
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
    protected HazelcastClientCacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        final HazelcastInstance instance;
        //uri is null or default or a non hazelcast one, then we use the internal shared instance
        if (uri == null || uri.equals(getDefaultURI())) {
            if (hazelcastInstance == null) {
                try {
                    hazelcastInstance = instanceFromProperties(classLoader, properties, true);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            instance = hazelcastInstance;
        } else {
            try {
                instance = instanceFromProperties(classLoader, properties, false);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return new HazelcastClientCacheManager(this, instance, uri, classLoader, properties);
    }

    private HazelcastInstance instanceFromProperties(ClassLoader classLoader, Properties properties, boolean isDefault)
            throws URISyntaxException, IOException {
        ClassLoader theClassLoader = classLoader == null ? getDefaultClassLoader() : classLoader;
        HazelcastInstance instance = null;
        String location = properties.getProperty(HazelcastCachingProvider.HAZELCAST_CONFIG_LOCATION);
        if (location != null) {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            if (scheme == null) {
                //it is a place holder
                uri = new URI(System.getProperty(uri.getRawSchemeSpecificPart()));
            }
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
                instance = HazelcastClient.newHazelcastClient(config);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        String instanceName = properties.getProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME);
        if (instanceName != null) {
            instance = HazelcastClient.getHazelcastClientByName(instanceName);
        }
        if (isDefault) {
            instance = HazelcastClient.newHazelcastClient();
        }
        return instance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastClientCachingProvider{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append('}');
        return sb.toString();
    }
}
