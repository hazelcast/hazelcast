/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.util.PropertiesHelper;

import java.io.IOException;
import java.util.Properties;

/**
 * A factory implementation to build up a {@link com.hazelcast.core.HazelcastInstance}
 * implementation using {@link com.hazelcast.client.HazelcastClient}.
 */
class HazelcastClientLoader implements IHazelcastInstanceLoader {

    private static final int CONNECTION_ATTEMPT_LIMIT = 10;

    private static final ILogger LOGGER = Logger.getLogger(HazelcastInstanceFactory.class);

    private HazelcastInstance client;
    private ClientConfig clientConfig;

    @Override
    public void configure(Properties props) {

        String address = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_ADDRESS, props, null);
        String group = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_GROUP, props, null);
        String pass = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_PASSWORD, props, null);
        String configResourcePath = CacheEnvironment.getConfigFilePath(props);

        if (configResourcePath != null) {
            try {
                clientConfig = new XmlClientConfigBuilder(configResourcePath).build();
            } catch (IOException e) {
                throw new HazelcastException("Could not load client configuration: " + configResourcePath, e);
            }
        } else {
            clientConfig = new ClientConfig();
        }

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
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(CONNECTION_ATTEMPT_LIMIT);
    }

    @Override
    public HazelcastInstance loadInstance() throws CacheException {
        client = HazelcastClient.newHazelcastClient(clientConfig);
        return client;
    }

    @Override
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
}
