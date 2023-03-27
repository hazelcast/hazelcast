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
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.datalink.impl.HazelcastDataLinkClientConfigBuilder;
import com.hazelcast.datalink.impl.HazelcastDataLinkFileReader;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Creates a HazelcastInstance that is shared to connect to a remote cluster.
 * <p>
 * Set the client XML as property using {@link HazelcastDataLink#CLIENT_XML} as property key or
 * Set the client YAML as property using {@link HazelcastDataLink#CLIENT_YML} as property key
 *
 * @since 5.3
 */
@Beta
public class HazelcastDataLink extends DataLinkBase {

    /**
     * The constant to be used as property key for XML string for connecting to remote cluster
     */
    public static final String CLIENT_XML = "client_xml";

    /**
     * The constant to be used as property key for YAML string for connecting to remote cluster
     */
    public static final String CLIENT_YML = "client_yml";

    /**
     * The constant to be used as property key for XML file path for connecting to remote cluster
     */
    public static final String CLIENT_XML_PATH = "client_xml_path";

    /**
     * The constant to be used as property key for YAML file path for connecting to remote cluster
     */
    public static final String CLIENT_YAML_PATH = "client_yaml_path";

    private final ClientConfig clientConfig;

    /**
     * The cached client instance
     */
    private volatile HazelcastClientProxy proxy;

    public HazelcastDataLink(@Nonnull DataLinkConfig dataLinkConfig) {
        super(dataLinkConfig);
        this.clientConfig = buildClientConfig();

        if (dataLinkConfig.isShared()) {
            HazelcastClientProxy hazelcastClientProxy = (HazelcastClientProxy) HazelcastClient
                    .newHazelcastClient(clientConfig);
            this.proxy = new HazelcastClientProxy(hazelcastClientProxy.client) {
                @Override
                public void shutdown() {
                    release();
                }
            };
        }
    }

    private ClientConfig buildClientConfig() {
        DataLinkConfig dataLinkConfig = getConfig();

        HazelcastDataLinkFileReader fileReader = new HazelcastDataLinkFileReader();
        fileReader.readFilePathIfProvided(dataLinkConfig);

        HazelcastDataLinkClientConfigBuilder configReader = new HazelcastDataLinkClientConfigBuilder();
        return configReader.buildClientConfig(dataLinkConfig);
    }

    @Nonnull
    @Override
    public Collection<DataLinkResource> listResources() {
        HazelcastInstance instance = getClient();
        try {
            return instance.getDistributedObjects()
                    .stream()
                    .filter(IMap.class::isInstance)
                    .map(o -> new DataLinkResource("IMap", o.getName()))
                    .collect(Collectors.toList());
        } finally {
            instance.shutdown();
        }
    }

    /**
     * Return a client {@link HazelcastInstance} based on this DataLink configuration.
     * <p>
     * Depending on the {@link DataLinkConfig#isShared()} config the HazelcastInstance is
     * - shared=true -> a single instance is created
     * - shared=false -> a new instance is created each time the method is called
     * <p>
     * The caller must call `shutdown` on the instance when finished to allow correct
     * release of the underlying resources. In case of a shared instance, the instance
     * is shut down when all users call the shutdown method and this DataLink is released.
     * In case of a non-shared instance, the instance is shutdown immediately
     * when the shutdown method is called.
     */
    @Nonnull
    public HazelcastInstance getClient() {
        if (getConfig().isShared()) {
            retain();
            return proxy;
        } else {
            return HazelcastClient.newHazelcastClient(clientConfig);
        }
    }

    @Override
    public synchronized void destroy() {
        if (proxy != null) {
            // the proxy has overridden shutdown method, need to close real client
            proxy.client.shutdown();
            proxy = null;
        }
    }
}
