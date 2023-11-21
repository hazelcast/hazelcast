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

package com.hazelcast.dataconnection;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.impl.hazelcastdataconnection.HazelcastDataConnectionClientConfigBuilder;
import com.hazelcast.dataconnection.impl.hazelcastdataconnection.HazelcastDataConnectionConfigLoader;
import com.hazelcast.dataconnection.impl.hazelcastdataconnection.HazelcastDataConnectionConfigValidator;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Creates a HazelcastInstance that is shared to connect to a remote cluster.
 * <p>
 * Set the client XML as property using {@link HazelcastDataConnection#CLIENT_XML} as property key or
 * Set the client YAML as property using {@link HazelcastDataConnection#CLIENT_YML} as property key
 *
 * @since 5.3
 */
public class HazelcastDataConnection extends DataConnectionBase {

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
    public static final String CLIENT_YML_PATH = "client_yml_path";

    /**
     * IMap Journal resource type name
     */
    public static final String OBJECT_TYPE_IMAP_JOURNAL = "IMapJournal";

    private final ClientConfig clientConfig;

    /**
     * The cached client instance
     */
    private ConcurrentMemoizingSupplier<HazelcastClientProxy> proxy;

    public HazelcastDataConnection(@Nonnull DataConnectionConfig dataConnectionConfig) {
        super(dataConnectionConfig);
        this.clientConfig = buildClientConfig();

        if (dataConnectionConfig.isShared()) {

            this.proxy = new ConcurrentMemoizingSupplier<>(() -> {
                HazelcastClientProxy hazelcastClientProxy = (HazelcastClientProxy) HazelcastClient
                        .newHazelcastClient(clientConfig);
                return new HazelcastClientProxy(hazelcastClientProxy.client) {
                    @Override
                    public void shutdown() {
                        release();
                    }
                };
            });
        }
    }

    private ClientConfig buildClientConfig() {
        validateConfiguration();

        HazelcastDataConnectionConfigLoader configLoader = new HazelcastDataConnectionConfigLoader();
        DataConnectionConfig dataConnectionConfig = configLoader.load(getConfig());

        HazelcastDataConnectionClientConfigBuilder configBuilder = new HazelcastDataConnectionClientConfigBuilder();
        return configBuilder.buildClientConfig(dataConnectionConfig);
    }

    private void validateConfiguration() {
        HazelcastDataConnectionConfigValidator validator = new HazelcastDataConnectionConfigValidator();
        DataConnectionConfig dataConnectionConfig = getConfig();
        validator.validate(dataConnectionConfig);
    }

    @Nonnull
    @Override
    public Collection<String> resourceTypes() {
        return Collections.singleton(OBJECT_TYPE_IMAP_JOURNAL);
    }

    @Nonnull
    @Override
    public Collection<DataConnectionResource> listResources() {
        HazelcastInstance instance = getClient();
        try {
            return instance.getDistributedObjects()
                    .stream()
                    .filter(IMap.class::isInstance)
                    .map(o -> new DataConnectionResource(OBJECT_TYPE_IMAP_JOURNAL, o.getName()))
                    .collect(Collectors.toList());
        } finally {
            instance.shutdown();
        }
    }

    /**
     * Return a client {@link HazelcastInstance} based on this data connection configuration.
     * <p>
     * Depending on the {@link DataConnectionConfig#isShared()} config the HazelcastInstance is
     * - shared=true -> a single instance is created
     * - shared=false -> a new instance is created each time the method is called
     * <p>
     * The caller must call `shutdown` on the instance when finished to allow correct
     * release of the underlying resources. In case of a shared instance, the instance
     * is shut down when all users call the shutdown method and this DataConnection is released.
     * In case of a non-shared instance, the instance is shutdown immediately
     * when the shutdown method is called.
     */
    @Nonnull
    public HazelcastInstance getClient() {
        if (getConfig().isShared()) {
            retain();
            return proxy.get();
        } else {
            return HazelcastClient.newHazelcastClient(clientConfig);
        }
    }

    @Override
    public synchronized void destroy() {
        if (proxy != null) {
            HazelcastClientProxy rememberedProxy = proxy.remembered();
            if (rememberedProxy != null) {
                // the proxy has overridden shutdown method, need to close real client
                rememberedProxy.client.shutdown();
            }
            proxy = null;
        }
    }
}
