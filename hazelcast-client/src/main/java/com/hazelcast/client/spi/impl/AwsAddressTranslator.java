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
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * AwsLookupTable loads ec2 ip addresses with given aws credentials.
 * Keeps a lookup table of private to public ip addresses.
 */
public class AwsAddressTranslator implements AddressTranslator {

    private final ILogger logger;
    private volatile Map<String, String> privateToPublic = new HashMap<String, String>();
    private final AWSClient awsClient;
    private final boolean isInsideAws;

    public AwsAddressTranslator(ClientAwsConfig awsConfig, LoggingService loggingService) {
        awsClient = new AWSClient(awsConfig);
        isInsideAws = awsConfig.isInsideAws();
        logger = loggingService.getLogger(AwsAddressTranslator.class);
    }

    /**
     * @param address
     * @return public address of network whose private address is given, if address
     * not founds returns null.
     */
    public Address translate(Address address) {
        if (isInsideAws) {
            return address;
        }
        if (address == null) {
            return null;
        }
        String publicAddress = getLookupTable().get(address.getHost());
        if (publicAddress != null) {
            return constructPrivateAddress(publicAddress, address);
        }

        // if it is already translated and cached, no need to refresh again.
        if (privateToPublic.values().contains(address.getHost())) {
            return address;
        }
        refresh();

        publicAddress = getLookupTable().get(address.getHost());
        if (publicAddress != null) {
            return constructPrivateAddress(publicAddress, address);
        }

        return null;
    }

    private Address constructPrivateAddress(String privateAddress, Address address) {
        try {
            return new Address(privateAddress, address.getPort());
        } catch (UnknownHostException e) {
            return null;
        }
    }

    private Map<String, String> getLookupTable() {
        return privateToPublic;
    }

    public void refresh() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }

}
