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

package com.hazelcast.client.impl;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.spi.impl.AwsAddressTranslator;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.logging.Level;

public class DefaultClientServiceFactory implements ClientServiceFactory {

    private static final ILogger LOGGER = Logger.getLogger(HazelcastClient.class);

    public DefaultClientServiceFactory() {
    }

    @Override
    public ClientConnectionManager createConnectionManager(ClientConfig config, HazelcastClientInstanceImpl client) {
        final ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();
        AddressTranslator addressTranslator;
        if (awsConfig != null && awsConfig.isEnabled()) {
            try {
                addressTranslator = new AwsAddressTranslator(awsConfig);
            } catch (NoClassDefFoundError e) {
                LOGGER.log(Level.WARNING, "hazelcast-cloud.jar might be missing!");
                throw e;
            }
        } else {
            addressTranslator = new DefaultAddressTranslator();
        }
        return new ClientConnectionManagerImpl(client, addressTranslator);
    }
}
