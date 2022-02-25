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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.impl.AbstractHazelcastCachingProvider;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;

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
 * Hazelcast client implementation of {@link javax.cache.spi.CachingProvider}.
 * <p>
 * Used internally by {@link com.hazelcast.cache.HazelcastCachingProvider} when
 * the JCache type is configured as {@code client}.
 * <p>
 * This implementation may create a new or reuse an existing {@link HazelcastInstance}
 * client.
 *
 * @see javax.cache.spi.CachingProvider
 */
public final class HazelcastClientCachingProvider extends AbstractHazelcastCachingProvider {

    public HazelcastClientCachingProvider() {
    }

    public HazelcastClientCachingProvider(HazelcastInstance instance) {
        this.hazelcastInstance = instance;
    }

    @Override
    public String toString() {
        return "HazelcastClientCachingProvider{hazelcastInstance=" + hazelcastInstance + '}';
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends AbstractHazelcastCacheManager> T createCacheManager(HazelcastInstance instance, URI uri,
                                                                             ClassLoader classLoader, Properties properties) {
        return (T) new HazelcastClientCacheManager(this, instance, uri, classLoader, properties);
    }

    @Nonnull
    @Override
    protected HazelcastInstance getOrCreateFromUri(@Nonnull URI uri,
                                                            ClassLoader classLoader,
                                                            String instanceName)
            throws URISyntaxException, IOException {
        ClientConfig config = getConfigFromLocation(uri, classLoader, instanceName);
        return getOrCreateInstanceByConfig(config);
    }

    @Nonnull
    @Override
    protected HazelcastInstance getDefaultInstance() {
        if (hazelcastInstance == null) {
            // if there is no default instance in use (not created yet and not specified):
            // 1. locate default ClientConfig: if it specifies an instance name, get-or-create an instance by that name
            // 2. otherwise start a new Hazelcast client
            ClientConfig clientConfig = getDefaultClientConfig();
            if (isNullOrEmptyAfterTrim(clientConfig.getInstanceName())) {
                hazelcastInstance = HazelcastClient.newHazelcastClient();
            } else {
                hazelcastInstance = getOrCreateInstanceByConfig(clientConfig);
            }
        }
        return hazelcastInstance;
    }

    @Override
    protected HazelcastInstance getOrCreateByInstanceName(String instanceName) {
        HazelcastInstance instance = HazelcastClient.getHazelcastClientByName(instanceName);
        if (instance == null) {
            ClientConfig clientConfig = getDefaultClientConfig();
            clientConfig.setInstanceName(instanceName);
            instance = HazelcastClient.newHazelcastClient(clientConfig);
        }
        return instance;
    }

    private ClientConfig getDefaultClientConfig() {
        ClientConfig clientConfig = new XmlClientConfigBuilder().build();
        if (namedDefaultHzInstance && isNullOrEmpty(clientConfig.getInstanceName())) {
            clientConfig.setInstanceName(HazelcastCachingProvider.SHARED_JCACHE_INSTANCE_NAME);
        }
        return clientConfig;
    }

    private ClientConfig getConfigFromLocation(URI location, ClassLoader classLoader, String instanceName)
            throws URISyntaxException, IOException {
        ClassLoader classLoaderOrDefault = classLoader == null ? getDefaultClassLoader() : classLoader;
        URL configURL = getConfigURL(location, classLoaderOrDefault);
        try {
            return getConfig(configURL, classLoaderOrDefault, instanceName);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ClientConfig getConfig(URL configURL, ClassLoader theClassLoader, String instanceName) throws IOException {
        ClientConfig config = new XmlClientConfigBuilder(configURL).build()
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

    private HazelcastInstance getOrCreateInstanceByConfig(ClientConfig config) {
        // lookup by config.getInstanceName(), if not found return a new HazelcastInstance
        HazelcastInstance instance = HazelcastClient.getHazelcastClientByName(config.getInstanceName());
        if (instance == null) {
            instance = HazelcastClient.newHazelcastClient(config);
        }
        return instance;
    }
}
