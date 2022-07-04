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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.impl.spi.impl.TranslateToPublicAddressProvider;
import com.hazelcast.client.impl.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.impl.spi.impl.discovery.RemoteAddressProvider;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.security.StaticCredentialsFactory;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.config.DiscoveryConfigReadOnly;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.client.properties.ClientProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

class ClusterDiscoveryServiceBuilder {

    private final int configsTryCount;
    private final LoggingService loggingService;
    private final HazelcastProperties properties;
    private final ClientExtension clientExtension;
    private final Collection<ClientConfig> configs;
    private final LifecycleService lifecycleService;
    private final AddressProvider externalAddressProvider;
    private final ClientClusterService clusterService;

    ClusterDiscoveryServiceBuilder(int configsTryCount, List<ClientConfig> configs, LoggingService loggingService,
                                   AddressProvider externalAddressProvider, HazelcastProperties properties,
                                   ClientExtension clientExtension, LifecycleService lifecycleService,
                                   ClientClusterService clusterService) {
        this.configsTryCount = configsTryCount;
        this.configs = configs;
        this.loggingService = loggingService;
        this.externalAddressProvider = externalAddressProvider;
        this.properties = properties;
        this.clientExtension = clientExtension;
        this.lifecycleService = lifecycleService;
        this.clusterService = clusterService;
    }

    public ClusterDiscoveryService build() {
        ArrayList<CandidateClusterContext> contexts = new ArrayList<>();
        for (ClientConfig config : configs) {
            ClientNetworkConfig networkConfig = config.getNetworkConfig();
            SocketInterceptor interceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
            ICredentialsFactory credentialsFactory = initCredentialsFactory(config);
            if (credentialsFactory == null) {
                credentialsFactory = new StaticCredentialsFactory(new UsernamePasswordCredentials(null, null));
            }
            credentialsFactory.configure(new ClientCallbackHandler(config));
            DiscoveryService discoveryService = initDiscoveryService(config);
            AddressProvider provider;
            if (externalAddressProvider != null) {
                provider = externalAddressProvider;
            } else {
                provider = createAddressProvider(config, discoveryService);
            }

            final SSLConfig sslConfig = networkConfig.getSSLConfig();
            final SocketOptions socketOptions = networkConfig.getSocketOptions();
            contexts.add(new CandidateClusterContext(config.getClusterName(), provider,
                    discoveryService, credentialsFactory,
                    interceptor, clientExtension.createChannelInitializer(sslConfig, socketOptions)));
        }
        return new ClusterDiscoveryService(unmodifiableList(contexts), configsTryCount, lifecycleService);
    }

    private AddressProvider createAddressProvider(ClientConfig clientConfig, DiscoveryService discoveryService) {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        ClientCloudConfig cloudConfig = networkConfig.getCloudConfig();

        List<String> addresses = networkConfig.getAddresses();
        boolean addressListProvided = addresses.size() != 0;
        boolean awsDiscoveryEnabled = networkConfig.getAwsConfig() != null && networkConfig.getAwsConfig().isEnabled();
        boolean gcpDiscoveryEnabled = networkConfig.getGcpConfig() != null && networkConfig.getGcpConfig().isEnabled();
        boolean azureDiscoveryEnabled = networkConfig.getAzureConfig() != null && networkConfig.getAzureConfig().isEnabled();
        boolean kubernetesDiscoveryEnabled = networkConfig.getKubernetesConfig() != null
                && networkConfig.getKubernetesConfig().isEnabled();
        boolean eurekaDiscoveryEnabled = networkConfig.getEurekaConfig() != null && networkConfig.getEurekaConfig().isEnabled();
        boolean discoverySpiEnabled = discoverySpiEnabled(networkConfig);
        String cloudDiscoveryToken = properties.getString(HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudDiscoveryToken != null && cloudConfig.isEnabled()) {
            throw new IllegalStateException("Ambiguous hazelcast.cloud configuration. "
                    + "Both property based and client configuration based settings are provided for "
                    + "Hazelcast cloud discovery together. Use only one.");
        }
        boolean hazelcastCloudEnabled = cloudDiscoveryToken != null || cloudConfig.isEnabled();
        isDiscoveryConfigurationConsistent(addressListProvided, awsDiscoveryEnabled, gcpDiscoveryEnabled, azureDiscoveryEnabled,
                kubernetesDiscoveryEnabled, eurekaDiscoveryEnabled, discoverySpiEnabled, hazelcastCloudEnabled);

        if (hazelcastCloudEnabled) {
            String discoveryToken = cloudDiscoveryToken(cloudConfig, cloudDiscoveryToken);
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, discoveryToken);
            int connectionTimeoutMillis = getConnectionTimeoutMillis(networkConfig);
            HazelcastCloudDiscovery cloudDiscovery = new HazelcastCloudDiscovery(urlEndpoint, connectionTimeoutMillis);
            //We use the usePublic parameter as true always because on the cloud context hazelcast members and clients
            // are never in the same network even-tough they can be in the same group/zone etc.
            return new RemoteAddressProvider(cloudDiscovery::discoverNodes, true);
        } else if (networkConfig.getAddresses().isEmpty() && discoveryService != null) {
            return new RemoteAddressProvider(() -> discoverAddresses(discoveryService), usePublicAddress(clientConfig));
        }
        TranslateToPublicAddressProvider toPublicAddressProvider = new TranslateToPublicAddressProvider(networkConfig,
                properties,
                loggingService.getLogger(TranslateToPublicAddressProvider.class));
        clusterService.addMembershipListener(toPublicAddressProvider);
        return new DefaultAddressProvider(networkConfig, toPublicAddressProvider);
    }

    private Map<Address, Address> discoverAddresses(DiscoveryService discoveryService) {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");
        Map<Address, Address> privateToPublic = new HashMap<>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            privateToPublic.put(discoveryNode.getPrivateAddress(), discoveryNode.getPublicAddress());
        }
        return privateToPublic;
    }

    private boolean discoverySpiEnabled(ClientNetworkConfig networkConfig) {
        return (networkConfig.getDiscoveryConfig() != null && networkConfig.getDiscoveryConfig().isEnabled())
                || Boolean.parseBoolean(properties.getString(DISCOVERY_SPI_ENABLED));
    }

    private boolean usePublicAddress(ClientConfig config) {
        return properties.getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(config.getNetworkConfig()));
    }

    @SuppressWarnings({"checkstyle:booleanexpressioncomplexity", "checkstyle:npathcomplexity"})
    private void isDiscoveryConfigurationConsistent(boolean addressListProvided, boolean awsDiscoveryEnabled,
                                                    boolean gcpDiscoveryEnabled, boolean azureDiscoveryEnabled,
                                                    boolean kubernetesDiscoveryEnabled, boolean eurekaDiscoveryEnabled,
                                                    boolean discoverySpiEnabled, boolean hazelcastCloudEnabled) {
        int count = 0;
        if (addressListProvided) {
            count++;
        }
        if (awsDiscoveryEnabled) {
            count++;
        }
        if (gcpDiscoveryEnabled) {
            count++;
        }
        if (azureDiscoveryEnabled) {
            count++;
        }
        if (kubernetesDiscoveryEnabled) {
            count++;
        }
        if (eurekaDiscoveryEnabled) {
            count++;
        }
        if (discoverySpiEnabled) {
            count++;
        }
        if (hazelcastCloudEnabled) {
            count++;
        }
        if (count > 1) {
            throw new IllegalStateException("Only one discovery method can be enabled at a time. "
                    + "cluster members given explicitly : " + addressListProvided
                    + ", aws discovery: " + awsDiscoveryEnabled
                    + ", gcp discovery: " + gcpDiscoveryEnabled
                    + ", azure discovery: " + azureDiscoveryEnabled
                    + ", kubernetes discovery: " + kubernetesDiscoveryEnabled
                    + ", eureka discovery: " + eurekaDiscoveryEnabled
                    + ", discovery spi enabled : " + discoverySpiEnabled
                    + ", hazelcast.cloud enabled : " + hazelcastCloudEnabled);
        }
    }

    private static String cloudDiscoveryToken(ClientCloudConfig cloudConfig, String cloudDiscoveryToken) {
        if (cloudConfig.isEnabled()) {
            return cloudConfig.getDiscoveryToken();
        } else {
            return cloudDiscoveryToken;
        }
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        if (!properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED) && aliasedDiscoveryConfigs.isEmpty()
                && !config.getNetworkConfig().isAutoDetectionEnabled()) {
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = new DiscoveryConfigReadOnly(networkConfig.getDiscoveryConfig());

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        boolean isAutoDetectionEnabled = networkConfig.isAutoDetectionEnabled();
        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(config.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setDiscoveryConfig(discoveryConfig)
                .setAutoDetectionEnabled(isAutoDetectionEnabled);

        DiscoveryService discoveryService = factory.newDiscoveryService(settings);
        if (isAutoDetectionEnabled && isEmptyDiscoveryStrategies(discoveryService)) {
            return null;
        }
        discoveryService.start();
        return discoveryService;
    }

    private boolean isEmptyDiscoveryStrategies(DiscoveryService discoveryService) {
        return discoveryService instanceof DefaultDiscoveryService
                && !((DefaultDiscoveryService) discoveryService).getDiscoveryStrategies().iterator().hasNext();
    }

    private ICredentialsFactory initCredentialsFactory(ClientConfig config) {
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        return securityConfig.asCredentialsFactory(config.getClassLoader());
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            return clientExtension.createSocketInterceptor(sic);
        }
        return null;
    }

    private int getConnectionTimeoutMillis(ClientNetworkConfig networkConfig) {
        int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }
}
