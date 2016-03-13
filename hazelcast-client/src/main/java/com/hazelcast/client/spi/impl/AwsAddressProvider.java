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

package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

/**
 * Aws Address Loader. Calls aws api to load ip addresses related to given credentials.
 */
public class AwsAddressProvider implements AddressProvider {

    private final ILogger logger;
    private final AWSClient awsClient;
    private volatile Map<String, String> privateToPublic;

    public AwsAddressProvider(ClientAwsConfig awsConfig, LoggingService loggingService) {
        awsClient = new AWSClient(awsConfig);
        logger = loggingService.getLogger(AwsAddressProvider.class);
    }

    @Override
    public Collection<InetSocketAddress> loadAddresses() {
        updateLookupTable();
        final Map<String, String> lookupTable = getLookupTable();
        final Collection<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(lookupTable.size());

        for (String privateAddress : lookupTable.keySet()) {
            addresses.addAll(AddressHelper.getSocketAddresses(privateAddress));
        }
        return addresses;

    }

    private Map<String, String> getLookupTable() {
        Map<String, String> table = privateToPublic;
        return table != null ? table : Collections.<String, String>emptyMap();
    }

    private void updateLookupTable() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }
}
