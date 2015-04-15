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

package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

/**
 * AwsLookupTable loads ec2 ip addresses with given aws credentials.
 * Keeps a lookup table of private to public ip addresses.
 */
public class AwsAddressTranslator implements AddressTranslator {

    private static final ILogger LOGGER = Logger.getLogger(AwsAddressTranslator.class);
    private volatile Map<String, String> privateToPublic;
    private final AWSClient awsClient;
    private final boolean isInsideAws;

    public AwsAddressTranslator(ClientAwsConfig awsConfig) {
        awsClient = new AWSClient(awsConfig);
        isInsideAws = awsConfig.isInsideAws();
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
        Map<String, String> table = privateToPublic;
        return table != null ? table : Collections.<String, String>emptyMap();
    }

    public void refresh() {
        try {
            privateToPublic = awsClient.getAddresses();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Aws addresses are failed to load : " + e.getMessage());
        }
    }

}
