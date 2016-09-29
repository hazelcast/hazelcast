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

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

import java.util.logging.Level;

public class DefaultClientConnectionManagerFactory implements ClientConnectionManagerFactory {

    public DefaultClientConnectionManagerFactory() {
    }

    @Override
    public ClientConnectionManager createConnectionManager(ClientConfig config, HazelcastClientInstanceImpl client,
                                                           DiscoveryService discoveryService) {

        LoggingService loggingService = client.getLoggingService();
        ILogger logger = loggingService.getLogger(HazelcastClient.class);
        ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();
        AddressTranslator addressTranslator;
        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressTranslator = new AwsAddressTranslator(awsConfig, loggingService);
            } catch (NoClassDefFoundError e) {
                logger.log(Level.WARNING, "hazelcast-aws.jar might be missing!");
                throw e;
            }
        } else if (discoveryService != null) {
            addressTranslator = new DiscoveryAddressTranslator(discoveryService,
                    client.getProperties().getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED));
        } else {
            addressTranslator = new DefaultAddressTranslator();
        }
        return new ClientConnectionManagerImpl(client, addressTranslator);
    }
}
