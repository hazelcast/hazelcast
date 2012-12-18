/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.ClientConfigBuilder;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.CacheEnvironment;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.hibernate.cache.CacheException;
import org.hibernate.util.PropertiesHelper;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;

class HazelcastClientLoader implements IHazelcastInstanceLoader {

    private final static ILogger logger = Logger.getLogger(HazelcastInstanceFactory.class.getName());

    private final Properties props = new Properties();
    private HazelcastClient client;

    public void configure(Properties props) {
        this.props.putAll(props);
    }

    public HazelcastInstance loadInstance() throws CacheException {
        if (props == null) {
            throw new NullPointerException("Hibernate environment properties is null!");
        }
        if (client != null && client.getLifecycleService().isRunning()) {
            logger.log(Level.WARNING, "Current HazelcastClient is already active! Shutting it down...");
            unloadInstance();
        }
        String address = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_ADDRESS, props, null);
        if (address == null) {
            String[] hosts = PropertiesHelper.toStringArray(CacheEnvironment.NATIVE_CLIENT_HOSTS, ",", props);
            if (hosts != null && hosts.length > 0) {
                address = hosts[0];
                logger.log(Level.WARNING, "Hibernate property '" + CacheEnvironment.NATIVE_CLIENT_HOSTS + "' " +
                        "is deprecated, use '" + CacheEnvironment.NATIVE_CLIENT_ADDRESS + "' instead!");
            }
        }
        String group = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_GROUP, props, null);
        String pass = PropertiesHelper.getString(CacheEnvironment.NATIVE_CLIENT_PASSWORD, props, null);
        String configResourcePath = CacheEnvironment.getConfigFilePath(props);

        ClientConfig clientConfig = null;
        if (configResourcePath != null) {
            try {
                clientConfig = new ClientConfigBuilder(configResourcePath).build();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Could not load client configuration: " + configResourcePath, e);
            }
        }
        if (clientConfig == null) {
            clientConfig = new ClientConfig();
            clientConfig.setUpdateAutomatic(true);
            clientConfig.setInitialConnectionAttemptLimit(3);
            clientConfig.setReconnectionAttemptLimit(5);
        }
        if (group != null) {
            clientConfig.getGroupConfig().setName(group);
        }
        if (pass != null) {
            clientConfig.getGroupConfig().setPassword(pass);
        }
        if (address != null) {
            clientConfig.addAddress(address);
        }
        return (client = HazelcastClient.newHazelcastClient(clientConfig));
    }

    public void unloadInstance() throws CacheException {
        if (client == null) {
            return;
        }
        try {
            client.shutdown();
            client = null;
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }
}
