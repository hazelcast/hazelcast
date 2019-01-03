/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;

import java.util.HashMap;
import java.util.Map;

public class HazelcastCloudAddressTranslator implements AddressTranslator {

    private final ILogger logger;
    private final HazelcastCloudDiscovery hazelcastCloudDiscovery;

    private volatile Map<Address, Address> privateToPublic = new HashMap<Address, Address>();

    public HazelcastCloudAddressTranslator(String endpointUrl, int connectionTimeoutMillis, LoggingService loggingService) {
        this(new HazelcastCloudDiscovery(endpointUrl, connectionTimeoutMillis), loggingService);
    }

    // just for testing
    HazelcastCloudAddressTranslator(HazelcastCloudDiscovery hazelcastCloudDiscovery, LoggingService loggingService) {
        this.hazelcastCloudDiscovery = hazelcastCloudDiscovery;
        this.logger = loggingService.getLogger(HazelcastCloudAddressTranslator.class);
    }

    @Override
    public Address translate(Address address) {
        if (address == null) {
            return null;
        }
        Address publicAddress = privateToPublic.get(address);
        if (publicAddress != null) {
            return publicAddress;
        }

        refresh();

        publicAddress = privateToPublic.get(address);
        if (publicAddress != null) {
            return publicAddress;
        }

        return null;
    }

    @Override
    public void refresh() {
        try {
            privateToPublic = hazelcastCloudDiscovery.discoverNodes();
        } catch (Exception e) {
            logger.warning("Failed to load addresses from hazelcast.cloud : " + e.getMessage());
        }
    }

    // just for testing
    Map<Address, Address> getLookupTable() {
        return privateToPublic;
    }

}
