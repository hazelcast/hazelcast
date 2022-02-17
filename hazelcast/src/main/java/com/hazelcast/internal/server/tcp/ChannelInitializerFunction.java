/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.nio.ascii.TextChannelInitializer;
import com.hazelcast.internal.server.ServerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ChannelInitializerFunction implements Function<EndpointQualifier, ChannelInitializer> {

    protected final ServerContext serverContext;
    private final ChannelInitializer uniChannelInitializer;
    private final Config config;
    private volatile Map<EndpointQualifier, ChannelInitializer> initializerMap;

    public ChannelInitializerFunction(ServerContext serverContext, Config config) {
        checkSslConfigAvailability(config);
        this.serverContext = serverContext;
        this.uniChannelInitializer = new UnifiedChannelInitializer(serverContext);
        this.config = config;
    }

    @Override
    public ChannelInitializer apply(EndpointQualifier qualifier) {
        return initializerMap.isEmpty() ? provideUnifiedChannelInitializer() : initializerMap.get(qualifier);
    }

    public void init() {
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        if (!advancedNetworkConfig.isEnabled() || advancedNetworkConfig.getEndpointConfigs().isEmpty()) {
            initializerMap = Collections.emptyMap();
            return;
        }

        Map<EndpointQualifier, ChannelInitializer> map = new HashMap<EndpointQualifier, ChannelInitializer>();
        for (EndpointConfig endpointConfig : advancedNetworkConfig.getEndpointConfigs().values()) {
            checkSslConfigAvailability(endpointConfig.getSSLConfig());

            switch (endpointConfig.getProtocolType()) {
                case MEMBER:
                    map.put(EndpointQualifier.MEMBER, provideMemberChannelInitializer(endpointConfig));
                    break;
                case CLIENT:
                    map.put(EndpointQualifier.CLIENT, provideClientChannelInitializer(endpointConfig));
                    break;
                case REST:
                    map.put(EndpointQualifier.REST, provideTextChannelInitializer(endpointConfig, true));
                    break;
                case MEMCACHE:
                    map.put(EndpointQualifier.MEMCACHE, provideTextChannelInitializer(endpointConfig, false));
                    break;
                case WAN:
                    map.put(endpointConfig.getQualifier(), provideMemberChannelInitializer(endpointConfig));
                    break;
                default:
                    throw new IllegalStateException("Cannot build channel initializer for protocol type "
                            + endpointConfig.getProtocolType());
            }
        }

        initializerMap = map;
    }

    protected ChannelInitializer provideUnifiedChannelInitializer() {
        return uniChannelInitializer;
    }

    protected ChannelInitializer provideMemberChannelInitializer(EndpointConfig endpointConfig) {
        return new MemberChannelInitializer(serverContext, endpointConfig);
    }

    protected ChannelInitializer provideClientChannelInitializer(EndpointConfig endpointConfig) {
        return new ClientChannelInitializer(serverContext, endpointConfig);
    }

    protected ChannelInitializer provideTextChannelInitializer(EndpointConfig endpointConfig, boolean rest) {
        return new TextChannelInitializer(serverContext, endpointConfig, rest);
    }

    protected ChannelInitializer provideWanChannelInitializer(EndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("TODO");
    }

    // check SSL config for unisocket member configuration
    private void checkSslConfigAvailability(Config config) {
        if (config.getAdvancedNetworkConfig().isEnabled()) {
            return;
        }
        SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
        checkSslConfigAvailability(sslConfig);
    }

    // check given SSL config
    private void checkSslConfigAvailability(SSLConfig sslConfig) {
        if (sslConfig != null && sslConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("SSL/TLS requires Hazelcast Enterprise Edition");
            }
        }
    }
}
