/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Hazelcast server implementation of {@link javax.cache.spi.CachingProvider}.
 * <p>
 * Used internally by {@link com.hazelcast.cache.HazelcastCachingProvider} when
 * the JCache type is configured as {@code server}.
 * <p>
 * This implementation may create a new or reuse an existing {@link HazelcastInstance}
 * member.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastServerCachingProvider extends AbstractHazelcastCachingProvider {

    public HazelcastServerCachingProvider() {
    }

    /**
     * Creates a new {@code HazelcastServerCachingProvider} with an
     * existing default fallback {@code HazelcastInstance}.
     *
     * @param instance  an existing, running {@code HazelcastInstance} which will be used
     *                  as default if no other means to identify a {@code HazelcastInstance}
     *                  are provided when constructing a {@code CacheManager} from this
     *                  {@code CachingProvider}.
     */
    public HazelcastServerCachingProvider(HazelcastInstance instance) {
        this.hazelcastInstance = instance;
    }

    @Override
    public String toString() {
        return "HazelcastServerCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends AbstractHazelcastCacheManager> T createCacheManager(HazelcastInstance instance, URI uri,
                                                                             ClassLoader classLoader, Properties properties) {
        return (T) new HazelcastServerCacheManager(this, instance, uri, classLoader, properties);
    }

    @Nonnull
    @Override
    protected HazelcastInstance getOrCreateFromUri(@Nonnull URI uri,
                                                   ClassLoader classLoader,
                                                   String instanceName)
            throws URISyntaxException, IOException {
        Config config = getConfigFromLocation(uri, classLoader, instanceName);
        return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
    }

    @Nonnull
    @Override
    protected HazelcastInstance getDefaultInstance() {
        if (hazelcastInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default Config: if it specifies an instance name, get-or-create an instance by that name
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

    @Override
    protected HazelcastInstance getOrCreateByInstanceName(String instanceName) {
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
            config.setInstanceName(HazelcastCachingProvider.SHARED_JCACHE_INSTANCE_NAME);
        }
        return config;
    }

    private Config getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        ClassLoader classLoaderOrDefault = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL = getConfigURL(location, classLoaderOrDefault);
        try {
            return getConfig(configURL, classLoaderOrDefault, instanceName);
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
}
