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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.integration.DiscoveryService;

public class CandidateClusterContext {

    private final AddressProvider addressProvider;
    private final AddressTranslator addressTranslator;
    private final DiscoveryService discoveryService;
    private final ICredentialsFactory credentialsFactory;

    public CandidateClusterContext(AddressProvider addressProvider,
                                   AddressTranslator addressTranslator,
                                   ICredentialsFactory credentialsFactory) {
        this.addressProvider = addressProvider;
        this.addressTranslator = addressTranslator;
        this.credentialsFactory = credentialsFactory;
        this.discoveryService = null;
    }

    CandidateClusterContext(AddressProvider addressProvider,
                            AddressTranslator addressTranslator,
                            DiscoveryService discoveryService,
                            ICredentialsFactory credentialsFactory) {
        this.addressProvider = addressProvider;
        this.addressTranslator = addressTranslator;
        this.discoveryService = discoveryService;
        this.credentialsFactory = credentialsFactory;
    }

    public void start() {
        if (discoveryService != null) {
            discoveryService.start();
        }
    }

    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
    }

    public void destroy() {
        if (discoveryService != null) {
            discoveryService.destroy();
        }
        credentialsFactory.destroy();
    }

    public AddressProvider getAddressProvider() {
        return addressProvider;
    }

    public AddressTranslator getAddressTranslator() {
        return addressTranslator;
    }


    public String getName() {
        return credentialsFactory.newCredentials().getPrincipal();
    }
}
