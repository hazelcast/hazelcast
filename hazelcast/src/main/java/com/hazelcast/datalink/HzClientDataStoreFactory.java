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

package com.hazelcast.datalink;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.StringUtil;

import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfigFromYaml;

/**
 * Creates a HazelcastInstance data store.
 * <p>
 * Set the client XML as property using {@link HzClientDataStoreFactory#CLIENT_XML} as property key or
 * Set the client YAML as property using {@link HzClientDataStoreFactory#CLIENT_YML} as property key
 */
public class HzClientDataStoreFactory implements DataLinkFactory<HazelcastInstance> {

    /**
     * The constant to be used as property key for XML
     */
    public static final String CLIENT_XML = "client_xml";

    /**
     * The constant to be used as property key for YAML
     */
    public static final String CLIENT_YML = "client_yml";

    // Reference to configuration
    private DataLinkConfig dataLinkConfig;

    // The cached client instance
    private HazelcastInstance hazelcastInstance;

    @Override
    public synchronized HazelcastInstance getDataLink() {
        if (hazelcastInstance == null) {

            ClientConfig clientConfig = buildClientConfig();

            if (clientConfig != null) {
                hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
                return hazelcastInstance;
            }
            throw new HazelcastException("XML or YAML in HzClientDataStoreFactory is null");
        }
        return hazelcastInstance;
    }

    @Override
    public void init(DataLinkConfig dataLinkConfig) {
        this.dataLinkConfig = dataLinkConfig;
    }

    @Override
    public boolean testConnection() {
        return true;
    }

    @Override
    public synchronized void close() throws Exception {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
            // Assign to null, so that we don't shut it down again
            hazelcastInstance = null;
        }
    }

    private ClientConfig buildClientConfig() {
        ClientConfig clientConfig = null;
        String clientXml = dataLinkConfig.getProperty(CLIENT_XML);
        if (!StringUtil.isNullOrEmpty(clientXml)) {
            // Read ClientConfig from XML
            clientConfig = asClientConfig(clientXml);
        }

        String clientYaml = dataLinkConfig.getProperty(CLIENT_YML);
        if (!StringUtil.isNullOrEmpty(clientYaml)) {
            // Read ClientConfig from Yaml
            clientConfig = asClientConfigFromYaml(clientYaml);
        }
        return clientConfig;
    }
}
