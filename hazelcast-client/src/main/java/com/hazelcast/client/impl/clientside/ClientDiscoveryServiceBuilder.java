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

import com.hazelcast.client.config.ClientCloudConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.connection.AddressTranslator;
import com.hazelcast.client.connection.nio.DefaultCredentialsFactory;
import com.hazelcast.client.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.spi.impl.DefaultAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressProvider;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressProvider;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudAddressTranslator;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.function.Function;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.client.spi.properties.ClientProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.client.spi.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.config.AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfig;
import static com.hazelcast.util.ExceptionUtil.rethrow;

class ClientDiscoveryServiceBuilder {

    private final ClientConfig clientConfig;
    private final LoggingService loggingService;
    private final AddressProvider externalAddressProvider;
    private final HazelcastProperties properties;
    private final Collection<ClientConfig> alternativeClientConfigs;
    private final Map<String, ICredentialsFactory> credentialsFactories = new HashMap<String, ICredentialsFactory>();

    ClientDiscoveryServiceBuilder(ClientConfig clientConfig, LoggingService loggingService,
                                  AddressProvider externalAddressProvider, HazelcastProperties properties) {
        this.clientConfig = clientConfig;
        this.alternativeClientConfigs = clientConfig.getAlternativeClientConfigs();
        this.loggingService = loggingService;
        this.externalAddressProvider = externalAddressProvider;
        this.properties = properties;
    }

    public ClientDiscoveryService build() {
        ArrayList<CandidateClusterContext> contexts = new ArrayList<CandidateClusterContext>();

        boolean addressListProvided = contexts.addAll(viaAddresses());
        boolean awsEnabled = contexts.addAll(viaDiscovery(new Function<ClientConfig, AliasedDiscoveryConfig>() {
            @Override
            public AliasedDiscoveryConfig apply(ClientConfig clientConfig) {
                return clientConfig.getNetworkConfig().getAwsConfig();
            }
        }));
        boolean gcpEnabled = contexts.addAll(viaDiscovery(new Function<ClientConfig, AliasedDiscoveryConfig>() {
            @Override
            public AliasedDiscoveryConfig apply(ClientConfig clientConfig) {
                return clientConfig.getNetworkConfig().getGcpConfig();
            }
        }));
        boolean azureEnabled = contexts.addAll(viaDiscovery(new Function<ClientConfig, AliasedDiscoveryConfig>() {
            @Override
            public AliasedDiscoveryConfig apply(ClientConfig clientConfig) {
                return clientConfig.getNetworkConfig().getAzureConfig();
            }
        }));
        boolean kubernetesEnabled = contexts.addAll(viaDiscovery(new Function<ClientConfig, AliasedDiscoveryConfig>() {
            @Override
            public AliasedDiscoveryConfig apply(ClientConfig clientConfig) {
                return clientConfig.getNetworkConfig().getKubernetesConfig();
            }
        }));
        boolean eurekaEnabled = contexts.addAll(viaDiscovery(new Function<ClientConfig, AliasedDiscoveryConfig>() {
            @Override
            public AliasedDiscoveryConfig apply(ClientConfig clientConfig) {
                return clientConfig.getNetworkConfig().getEurekaConfig();
            }
        }));
        boolean hazelcastCloudEnabled = contexts.addAll(viaCloudDiscovery());

        boolean discoverySpiEnabled = discoverySpiEnabled();
        if (discoverySpiEnabled) {
            contexts.addAll(viaDiscoverySpi());
        }

        //no specific discovery configuration, first check if mock network is used
        if (contexts.isEmpty()) {
            contexts.addAll(viaExternalAddressProvider());
        }

        //no specific discovery configuration and no mock network, add default addresses localhost:5701,...
        if (contexts.isEmpty()) {
            contexts.addAll(viaDefaultAddresses());
        }
        isDiscoveryConfigurationConsistent(addressListProvided, awsEnabled,
                gcpEnabled, azureEnabled, kubernetesEnabled, eurekaEnabled,
                discoverySpiEnabled, hazelcastCloudEnabled);

        return new ClientDiscoveryService(contexts);
    }

    private List<CandidateClusterContext> viaDefaultAddresses() {
        List<CandidateClusterContext> clusterContexts = new ArrayList<CandidateClusterContext>();

        DefaultAddressProvider provider = new DefaultAddressProvider();
        DefaultAddressTranslator translator = new DefaultAddressTranslator();
        clusterContexts.add(new CandidateClusterContext(provider, translator, findCredentialsFactory(clientConfig)));

        for (ClientConfig config : alternativeClientConfigs) {
            ICredentialsFactory credentialsFactory = findCredentialsFactory(config);
            clusterContexts.add(new CandidateClusterContext(provider, translator, credentialsFactory));
        }
        return clusterContexts;

    }

    private List<CandidateClusterContext> viaExternalAddressProvider() {
        if (externalAddressProvider == null) {
            return Collections.EMPTY_LIST;
        }

        DefaultAddressTranslator translator = new DefaultAddressTranslator();
        ICredentialsFactory credentialsFactory = findCredentialsFactory(clientConfig);
        CandidateClusterContext ctxt = new CandidateClusterContext(externalAddressProvider, translator, credentialsFactory);
        return Collections.singletonList(ctxt);
    }

    private List<CandidateClusterContext> viaAddresses() {
        List<CandidateClusterContext> clusterContexts = new ArrayList<CandidateClusterContext>();

        List<String> addresses = clientConfig.getNetworkConfig().getAddresses();
        if (addresses.size() != 0) {
            DefaultAddressProvider provider = new DefaultAddressProvider(addresses);
            DefaultAddressTranslator translator = new DefaultAddressTranslator();
            ICredentialsFactory credentialsFactory = findCredentialsFactory(clientConfig);
            CandidateClusterContext context = new CandidateClusterContext(provider, translator, credentialsFactory);
            clusterContexts.add(context);
        }

        for (ClientConfig config : alternativeClientConfigs) {
            List<String> configuredAddresses = config.getNetworkConfig().getAddresses();
            if (configuredAddresses.size() == 0) {
                continue;
            }

            ICredentialsFactory credentialsFactory = findCredentialsFactory(config);
            DefaultAddressProvider provider = new DefaultAddressProvider(configuredAddresses);
            DefaultAddressTranslator translator = new DefaultAddressTranslator();
            CandidateClusterContext context = new CandidateClusterContext(provider, translator, credentialsFactory);
            clusterContexts.add(context);
        }

        return clusterContexts;
    }

    private DiscoveryService initDiscoveryServiceViaSPI(DiscoveryConfig discoveryConfig) {

        ILogger logger = loggingService.getLogger(DiscoveryService.class);

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(clientConfig.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setDiscoveryConfig(discoveryConfig);

        return factory.newDiscoveryService(settings);
    }

    private List<CandidateClusterContext> viaDiscoverySpi() {
        boolean usePublic = usePublicAddressSPI(properties);
        List<CandidateClusterContext> clusterContexts = new ArrayList<CandidateClusterContext>();

        DiscoveryConfig discoveryConfig = clientConfig.getNetworkConfig().getDiscoveryConfig();
        if (discoveryConfig.isEnabled()) {
            DiscoveryService discoveryService = initDiscoveryServiceViaSPI(discoveryConfig);
            AddressProvider provider = new DiscoveryAddressProvider(discoveryService);
            AddressTranslator translator = new DiscoveryAddressTranslator(discoveryService, usePublic);
            CandidateClusterContext context =
                    new CandidateClusterContext(provider, translator, discoveryService, findCredentialsFactory(clientConfig));
            clusterContexts.add(context);
        }

        for (ClientConfig config : alternativeClientConfigs) {
            DiscoveryConfig disConfig = config.getNetworkConfig().getDiscoveryConfig();
            if (!disConfig.isEnabled()) {
                continue;
            }
            DiscoveryService discoveryService = initDiscoveryServiceViaSPI(disConfig);

            ICredentialsFactory factory = findCredentialsFactory(config);
            AddressProvider provider = new DiscoveryAddressProvider(discoveryService);
            AddressTranslator translator = new DiscoveryAddressTranslator(discoveryService, usePublic);
            CandidateClusterContext context = new CandidateClusterContext(provider, translator, discoveryService, factory);
            clusterContexts.add(context);
        }

        return clusterContexts;
    }


    private DiscoveryService initDiscoveryServiceAliased(AliasedDiscoveryConfig aliasedDiscoveryConfig) {
        DiscoveryStrategyConfig discoveryStrategyConfig = createDiscoveryStrategyConfig(aliasedDiscoveryConfig);
        ILogger logger = loggingService.getLogger(DiscoveryService.class);

        DiscoveryServiceProvider factory = new DefaultDiscoveryServiceProvider();

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(clientConfig.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setAliasedDiscoveryConfigs(Collections.singletonList(discoveryStrategyConfig))
                .setDiscoveryConfig(new DiscoveryConfig());

        return factory.newDiscoveryService(settings);
    }


    private List<CandidateClusterContext> viaDiscovery(Function<ClientConfig, AliasedDiscoveryConfig> accessor) {
        List<CandidateClusterContext> clusterContexts = new ArrayList<CandidateClusterContext>();

        AliasedDiscoveryConfig aliasedDiscoveryConfig = accessor.apply(clientConfig);
        if (aliasedDiscoveryConfig != null && aliasedDiscoveryConfig.isEnabled()) {
            DiscoveryService discoveryService = initDiscoveryServiceAliased(aliasedDiscoveryConfig);
            AddressProvider provider = new DiscoveryAddressProvider(discoveryService);
            AddressTranslator translator =
                    new DiscoveryAddressTranslator(discoveryService, aliasedDiscoveryConfig.isUsePublicIp());
            CandidateClusterContext context =
                    new CandidateClusterContext(provider, translator, discoveryService, findCredentialsFactory(clientConfig));
            clusterContexts.add(context);
        }

        for (ClientConfig config : alternativeClientConfigs) {
            AliasedDiscoveryConfig discoveryConfig = accessor.apply(config);
            if (!discoveryConfig.isEnabled()) {
                continue;
            }

            DiscoveryService discoveryService = initDiscoveryServiceAliased(discoveryConfig);

            ICredentialsFactory factory = findCredentialsFactory(config);
            AddressProvider provider = new DiscoveryAddressProvider(discoveryService);
            AddressTranslator translator = new DiscoveryAddressTranslator(discoveryService, discoveryConfig.isUsePublicIp());
            CandidateClusterContext context = new CandidateClusterContext(provider, translator, discoveryService, factory);
            clusterContexts.add(context);
        }

        return clusterContexts;
    }


    private List<CandidateClusterContext> viaCloudDiscovery() {
        List<CandidateClusterContext> clusterContexts = new ArrayList<CandidateClusterContext>();
        String cloudDiscoveryToken = properties.getString(HAZELCAST_CLOUD_DISCOVERY_TOKEN);

        ClientCloudConfig cloudConfig = clientConfig.getNetworkConfig().getCloudConfig();
        checkIfLegalCloudConfig(cloudDiscoveryToken, cloudConfig);
        if (isCloudEnabled(cloudDiscoveryToken, cloudConfig)) {
            AddressProvider addressProvider = initCloudAddressProvider(cloudConfig);
            AddressTranslator addressTranslator = initCloudAddressTranslator(cloudConfig, cloudDiscoveryToken);
            ICredentialsFactory credentialsFactory = findCredentialsFactory(clientConfig);
            clusterContexts.add(new CandidateClusterContext(addressProvider, addressTranslator, credentialsFactory));
        }

        for (ClientConfig config : alternativeClientConfigs) {
            ClientCloudConfig clientCloudConfig = config.getNetworkConfig().getCloudConfig();
            checkIfLegalCloudConfig(cloudDiscoveryToken, clientCloudConfig);

            if (!isCloudEnabled(cloudDiscoveryToken, clientCloudConfig)) {
                continue;
            }

            ICredentialsFactory credentialsFactory = findCredentialsFactory(config);
            AddressProvider addressProvider = initCloudAddressProvider(cloudConfig);
            AddressTranslator addressTranslator = initCloudAddressTranslator(cloudConfig, cloudDiscoveryToken);
            clusterContexts.add(new CandidateClusterContext(addressProvider, addressTranslator, credentialsFactory));
        }

        return clusterContexts;
    }

    private void checkIfLegalCloudConfig(String cloudDiscoveryToken, ClientCloudConfig cloudConfig) {
        if (cloudDiscoveryToken != null && cloudConfig.isEnabled()) {
            throw new IllegalStateException("Ambiguous hazelcast.cloud configuration. "
                    + "Both property based and client configuration based settings are provided for "
                    + "Hazelcast cloud discovery together. Use only one.");
        }
    }

    private boolean isCloudEnabled(String cloudDiscoveryToken, ClientCloudConfig cloudConfig) {
        return cloudDiscoveryToken != null || cloudConfig.isEnabled();
    }

    private int getConnectionTimeoutMillis() {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        int connTimeout = networkConfig.getConnectionTimeout();
        return connTimeout == 0 ? Integer.MAX_VALUE : connTimeout;
    }


    private HazelcastCloudAddressProvider initCloudAddressProvider(ClientCloudConfig cloudConfig) {
        if (cloudConfig.isEnabled()) {
            String discoveryToken = cloudConfig.getDiscoveryToken();
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, discoveryToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
        }

        String cloudToken = properties.getString(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN);
        if (cloudToken != null) {
            String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
            String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, cloudToken);
            return new HazelcastCloudAddressProvider(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
        }
        return null;
    }

    private AddressTranslator initCloudAddressTranslator(ClientCloudConfig cloudConfig, String cloudDiscoveryToken) {
        String discoveryToken;
        if (cloudConfig.isEnabled()) {
            discoveryToken = cloudConfig.getDiscoveryToken();
        } else {
            discoveryToken = cloudDiscoveryToken;
        }
        String cloudUrlBase = properties.getString(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY);
        String urlEndpoint = HazelcastCloudDiscovery.createUrlEndpoint(cloudUrlBase, discoveryToken);
        return new HazelcastCloudAddressTranslator(urlEndpoint, getConnectionTimeoutMillis(), loggingService);
    }


    private boolean discoverySpiEnabled() {
        return Boolean.parseBoolean(properties.getString(DISCOVERY_SPI_ENABLED));
    }

    private boolean usePublicAddressSPI(HazelcastProperties properties) {
        return properties.getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED);
    }

    @SuppressWarnings({"checkstyle:booleanexpressioncomplexity", "checkstyle:npathcomplexity", "checkstyle:parameternumber"})
    private void isDiscoveryConfigurationConsistent(boolean addressListProvided,
                                                    boolean awsDiscoveryEnabled, boolean gcpDiscoveryEnabled,
                                                    boolean azureDiscoveryEnabled, boolean kubernetesDiscoveryEnabled,
                                                    boolean eurekaDiscoveryEnabled, boolean discoverySpiEnabled,
                                                    boolean hazelcastCloudEnabled) {
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
                    + ", cluster members given explicitly : " + addressListProvided
                    + ", aws discovery: " + awsDiscoveryEnabled
                    + ", gcp discovery: " + gcpDiscoveryEnabled
                    + ", azure discovery: " + azureDiscoveryEnabled
                    + ", kubernetes discovery: " + kubernetesDiscoveryEnabled
                    + ", eureka discovery: " + eurekaDiscoveryEnabled
                    + ", discovery spi enabled : " + discoverySpiEnabled
                    + ", hazelcast.cloud enabled : " + hazelcastCloudEnabled);
        }
    }

    private ICredentialsFactory findCredentialsFactory(ClientConfig clientConfig) {
        String name = clientConfig.getGroupConfig().getName();
        ICredentialsFactory credentialsFactory = credentialsFactories.get(name);
        if (credentialsFactory != null) {
            return credentialsFactory;
        }

        credentialsFactory = initCredentialsFactory(clientConfig);
        credentialsFactories.put(name, credentialsFactory);
        return credentialsFactory;
    }

    private ICredentialsFactory initCredentialsFactory(ClientConfig config) {
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        validateSecurityConfig(securityConfig);
        ICredentialsFactory c = getCredentialsFromFactory(config);
        if (c == null) {
            return new DefaultCredentialsFactory(securityConfig, config.getGroupConfig(), config.getClassLoader());
        }
        return c;
    }

    private void validateSecurityConfig(ClientSecurityConfig securityConfig) {
        boolean configuredViaCredentials = securityConfig.getCredentials() != null
                || securityConfig.getCredentialsClassname() != null;

        CredentialsFactoryConfig factoryConfig = securityConfig.getCredentialsFactoryConfig();
        boolean configuredViaCredentialsFactory = factoryConfig.getClassName() != null
                || factoryConfig.getImplementation() != null;

        if (configuredViaCredentials && configuredViaCredentialsFactory) {
            throw new IllegalStateException("Ambiguous Credentials config. Set only one of Credentials or ICredentialsFactory");
        }
    }

    private ICredentialsFactory getCredentialsFromFactory(ClientConfig config) {
        CredentialsFactoryConfig credentialsFactoryConfig = config.getSecurityConfig().getCredentialsFactoryConfig();
        ICredentialsFactory factory = credentialsFactoryConfig.getImplementation();
        if (factory == null) {
            String factoryClassName = credentialsFactoryConfig.getClassName();
            if (factoryClassName != null) {
                try {
                    factory = ClassLoaderUtil.newInstance(config.getClassLoader(), factoryClassName);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        }
        if (factory == null) {
            return null;
        }
        factory.configure(config.getGroupConfig(), credentialsFactoryConfig.getProperties());
        return factory;
    }

}
