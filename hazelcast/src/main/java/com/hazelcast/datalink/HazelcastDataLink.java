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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfig;
import static com.hazelcast.jet.impl.util.ImdgUtil.asClientConfigFromYaml;

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
     * The constant to be used as property key for XML
     */
    public static final String CLIENT_XML = "client_xml";

    /**
     * The constant to be used as property key for YAML
     */
    public static final String CLIENT_YML = "client_yml";

    private final ClientConfig clientConfig;

    /**
     * The cached client instance
     */
    private volatile HazelcastClientProxy proxy;

    public HazelcastDataLink(@Nonnull DataLinkConfig dataLinkConfig) {
        super(dataLinkConfig);
        this.clientConfig = buildClientConfig();

        if (dataLinkConfig.isShared()) {
            HazelcastClientProxy proxy = (HazelcastClientProxy) HazelcastClient.newHazelcastClient(clientConfig);
            this.proxy = new HazelcastClientProxy(proxy.client) {
                @Override
                public void shutdown() {
                    release();
                }
            };
        }
    }

    private ClientConfig buildClientConfig() {
        ClientConfig clientConfig = null;
        String clientXml = getConfig().getProperty(CLIENT_XML);
        if (!StringUtil.isNullOrEmpty(clientXml)) {
            // Read ClientConfig from XML
            clientConfig = asClientConfig(clientXml);
        }

        String clientYaml = getConfig().getProperty(CLIENT_YML);
        if (!StringUtil.isNullOrEmpty(clientYaml)) {
            // Read ClientConfig from Yaml
            clientConfig = asClientConfigFromYaml(clientYaml);
        }

        if (clientConfig == null) {
            throw new HazelcastException("HazelcastDataLink with name '" + getConfig().getName()
                    + "' could not be created, provide either client_xml or client_yml property "
                    + "with the client configuration.");
        }
        return clientConfig;
    }

    @Nonnull
    @Override
    public Collection<DataLinkResource> listResources() {
        HazelcastInstance instance = getClient();
        try {
            return instance.getDistributedObjects()
                           .stream()
                           .filter(o -> o instanceof IMap)
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
