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

package com.hazelcast.hibernate.instance;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.internal.util.config.ConfigurationHelper;

import java.io.IOException;
import java.util.Properties;

/**
 * A factory implementation to build up a {@link com.hazelcast.core.HazelcastInstance}
 * implementation using {@link com.hazelcast.client.HazelcastClient}.
 */
class HazelcastClientLoader implements IHazelcastInstanceLoader {

    private static final int CONNECTION_ATTEMPT_LIMIT = 10;

    private static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceFactory.class);

    private final Properties props = new Properties();
    private HazelcastInstance client;

    public void configure(Properties props) {
        this.props.putAll(props);
    }

    public HazelcastInstance loadInstance() throws CacheException {
        if (client != null && client.getLifecycleService().isRunning()) {
            LOGGER.warning("Current HazelcastClient is already active! Shutting it down...");
            unloadInstance();
        }
        String address = ConfigurationHelper.getString(CacheEnvironment.NATIVE_CLIENT_ADDRESS, props, null);
        String group = ConfigurationHelper.getString(CacheEnvironment.NATIVE_CLIENT_GROUP, props, null);
        String pass = ConfigurationHelper.getString(CacheEnvironment.NATIVE_CLIENT_PASSWORD, props, null);
        String configResourcePath = CacheEnvironment.getConfigFilePath(props);

        ClientConfig clientConfig = buildClientConfig(configResourcePath);
        if (group != null) {
            clientConfig.getGroupConfig().setName(group);
        }
        if (pass != null) {
            clientConfig.getGroupConfig().setPassword(pass);
        }
        if (address != null) {
            clientConfig.getNetworkConfig().addAddress(address);
        }
        clientConfig.getNetworkConfig().setSmartRouting(true);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        client = HazelcastClient.newHazelcastClient(clientConfig);
        return client;
    }

    public void unloadInstance() throws CacheException {
        if (client == null) {
            return;
        }
        try {
            client.getLifecycleService().shutdown();
            client = null;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    private ClientConfig buildClientConfig(String configResourcePath) {
        ClientConfig clientConfig = null;
        if (configResourcePath != null) {
            try {
                clientConfig = new XmlClientConfigBuilder(configResourcePath).build();
            } catch (IOException e) {
                LOGGER.warning("Could not load client configuration: " + configResourcePath, e);
            }
        }
        if (clientConfig == null) {
            clientConfig = new ClientConfig();
            final ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
            networkConfig.setSmartRouting(true);
            networkConfig.setRedoOperation(true);
            networkConfig.setConnectionAttemptLimit(CONNECTION_ATTEMPT_LIMIT);
        }
        return clientConfig;
    }
}
