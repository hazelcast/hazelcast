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

    public HazelcastServerCachingProvider() {
        this(null);
    }

    private HazelcastServerCachingProvider(HazelcastInstance defaultHazelcastInstance) {
        super(defaultHazelcastInstance);
    }

    /**
     * Helper method for creating caching provider for testing, etc.
     *
     * @param hazelcastInstance
     *
     * @return HazelcastServerCachingProvider
     */
    public static HazelcastServerCachingProvider createCachingProvider(HazelcastInstance hazelcastInstance) {
        final HazelcastServerCachingProvider cachingProvider = new HazelcastServerCachingProvider(hazelcastInstance);
        return cachingProvider;
    }

    @Override
    protected HazelcastServerCacheManager createHazelcastCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        HazelcastInstance instance = null;
        //uri is null or default or a non hazelcast one, then we use the internal shared instance
        if (uri == null || uri.equals(getDefaultURI())) {
            if (defaultHazelcastInstance != null) {
                instance = defaultHazelcastInstance;
            } else if (hazelcastInstance == null) {
                try {
                    hazelcastInstance = instanceFromProperties(classLoader, properties, true);
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (instance == null) {
                instance = hazelcastInstance;
            }
        } else {
            try {
                instance = instanceFromProperties(classLoader, properties, false);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return new HazelcastServerCacheManager(this, instance, uri, classLoader, properties);
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
                Config config = new XmlConfigBuilder(configURL).build();
                config.setClassLoader(theClassLoader);
                //TODO should we assign an instance name
                config.setInstanceName(configURL.toString());
                instance = HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        String instanceName = properties.getProperty(HazelcastCachingProvider.HAZELCAST_INSTANCE_NAME);
        if (instanceName != null) {
            instance = Hazelcast.getHazelcastInstanceByName(instanceName);
        }
        if (isDefault || instance == null) {
            instance = getOrCreateDefaultHazelcastInstance();
        }
        return instance;
    }

    private HazelcastInstance getOrCreateDefaultHazelcastInstance() {
        HazelcastInstance instance;
        if (defaultHazelcastInstance != null) {
            instance = defaultHazelcastInstance;
        } else if (hazelcastInstance != null) {
            instance = hazelcastInstance;
        } else {
            Config config = new XmlConfigBuilder().build();
            if (config.getInstanceName() == null) {
                config.setInstanceName("JCacheSharedInstance");
            }
            instance = HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
        }
        return instance;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HazelcastServerCachingProvider{");
        sb.append("hazelcastInstance=").append(hazelcastInstance);
        sb.append('}');
        return sb.toString();
    }
}
