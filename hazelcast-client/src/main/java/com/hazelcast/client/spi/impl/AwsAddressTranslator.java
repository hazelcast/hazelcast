/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * AwsAddressTranslator loads EC2 IP addresses with given AWS credentials.
 *
 * Keeps a lookup table of private to public IP addresses.
 */
public class AwsAddressTranslator implements AddressTranslator {

    private final ILogger logger;
    private final AWSClient awsClient;
    private final boolean isInsideAws;

    private volatile Map<String, String> privateToPublic = new HashMap<String, String>();

    public AwsAddressTranslator(ClientAwsConfig awsConfig, LoggingService loggingService) {
        this(new AWSClient(awsConfig), awsConfig, loggingService);
    }

    // just for testing
    AwsAddressTranslator(AWSClient awsClient, ClientAwsConfig awsConfig, LoggingService loggingService) {
        this.awsClient = awsClient;
        this.isInsideAws = awsConfig.isInsideAws();
        this.logger = loggingService.getLogger(AwsAddressTranslator.class);
    }

    /**
     * Translates an IP address from the private AWS network to the public network.
     *
     * @param address the private address to translate
     * @return public address of network whose private address is given, if address not founds returns {@code null}.
     */
    @Override
    public Address translate(Address address) {
        if (isInsideAws) {
            return address;
        }
        if (address == null) {
            return null;
        }
        String publicAddress = privateToPublic.get(address.getHost());
        if (publicAddress != null) {
            return createAddressOrNull(publicAddress, address);
        }

        // if it's already translated and cached, there is no need to refresh again
        if (privateToPublic.values().contains(address.getHost())) {
            return address;
        }
        refresh();

        publicAddress = privateToPublic.get(address.getHost());
        if (publicAddress != null) {
            return createAddressOrNull(publicAddress, address);
        }

        return null;
    }

    @Override
    public void refresh() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            logger.warning("AWS addresses failed to load: " + e.getMessage());
        }
    }

    // just for testing
    Map<String, String> getLookupTable() {
        return privateToPublic;
    }

    private static Address createAddressOrNull(String hostAddress, Address portAddress) {
        try {
            return new Address(hostAddress, portAddress.getPort());
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
