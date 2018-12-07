/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.Addresses;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

public class HazelcastCloudAddressProvider implements AddressProvider {

    private final ILogger logger;
    private final HazelcastCloudDiscovery cloudDiscovery;

    public HazelcastCloudAddressProvider(String endpointUrl, int connectionTimeoutInMillis, LoggingService loggingService) {
        this(new HazelcastCloudDiscovery(endpointUrl, connectionTimeoutInMillis), loggingService);
    }

    HazelcastCloudAddressProvider(HazelcastCloudDiscovery cloudDiscovery, LoggingService loggingService) {
        this.cloudDiscovery = cloudDiscovery;
        this.logger = loggingService.getLogger(HazelcastCloudAddressProvider.class);
    }

    @Override
    public Addresses loadAddresses() {
        try {
            return new Addresses(cloudDiscovery.discoverNodes().keySet());
        } catch (Exception e) {
            logger.warning("Failed to load addresses from hazelcast.cloud : " + e.getMessage());
        }
        return new Addresses();
    }
}
