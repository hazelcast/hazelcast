/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastore;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ExternalDataStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;

import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;

/**
 * Creates a HazelcastInstance data store.
 * <p>
 * Set the client XML as property using {@link HzClientDataStoreFactory#CLIENT_XML} as property key
 *
 */
public class HzClientDataStoreFactory implements ExternalDataStoreFactory<HazelcastInstance> {

    /**
     * The constant to be used as property key
     */
    public static final String CLIENT_XML = "client_xml";

    // Reference to configuration
    private ExternalDataStoreConfig externalDataStoreConfig;

    // The cached client instance
    private HazelcastInstance hazelcastInstance;

    @Override
    public synchronized HazelcastInstance getDataStore() {
        if (hazelcastInstance == null) {

            String clientXml = externalDataStoreConfig.getProperty(CLIENT_XML);
            if (clientXml == null) {
                throw new HazelcastException("XML in HzClientDataStoreFactory is null");
            }
            // Create a client instance
            ClientConfig clientConfig = asClientConfig(clientXml);
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
        }
        return hazelcastInstance;
    }

    @Override
    public void init(ExternalDataStoreConfig externalDataStoreConfig) {
        this.externalDataStoreConfig = externalDataStoreConfig;
    }

    @Override
    public synchronized void close() throws Exception {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
            // Assign to null, so that we don't shut it down again
            hazelcastInstance = null;
        }
    }
}
