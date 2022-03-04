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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.internal.server.tcp.TcpServerConnectionChannelErrorHandler;
import com.hazelcast.internal.server.tcp.TcpServerContext;
import com.hazelcast.internal.util.InstantiationUtils;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.List;
import java.util.Properties;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;
import static com.hazelcast.spi.properties.ClusterProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.IO_OUTPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.ClusterProperty.IO_WRITE_THROUGH_ENABLED;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class DefaultNodeContext implements NodeContext {

    public static final List<String> EXTENSION_PRIORITY_LIST = unmodifiableList(asList(
            "com.hazelcast.instance.impl.EnterpriseNodeExtension",
            "com.hazelcast.instance.impl.DefaultNodeExtension"
    ));

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node, EXTENSION_PRIORITY_LIST);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        Config config = node.getConfig();
        MemberAddressProviderConfig memberAddressProviderConfig =
                getActiveMemberNetworkConfig(config).getMemberAddressProviderConfig();

        final ILogger addressPickerLogger = node.getLogger(AddressPicker.class);
        if (!memberAddressProviderConfig.isEnabled()) {
            if (config.getAdvancedNetworkConfig().isEnabled()) {
                return new AdvancedNetworkAddressPicker(config, addressPickerLogger);
            }

            return new DefaultAddressPicker(config, addressPickerLogger);
        }

        MemberAddressProvider implementation = memberAddressProviderConfig.getImplementation();
        if (implementation != null) {
            return new DelegatingAddressPicker(implementation, config, addressPickerLogger);
        }
        ClassLoader classLoader = config.getClassLoader();
        String classname = memberAddressProviderConfig.getClassName();
        Class<? extends MemberAddressProvider> clazz = loadMemberAddressProviderClass(classLoader, classname);
        ILogger memberAddressProviderLogger = node.getLogger(clazz);

        Properties properties = memberAddressProviderConfig.getProperties();
        MemberAddressProvider memberAddressProvider = newMemberAddressProviderInstance(clazz,
                memberAddressProviderLogger, properties);
        return new DelegatingAddressPicker(memberAddressProvider, config, addressPickerLogger);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    // justification for the suppression: the flow is pretty straightforward and breaking this up into smaller pieces
    // would make it harder to read
    private static MemberAddressProvider newMemberAddressProviderInstance(Class<? extends MemberAddressProvider> clazz,
                                                                          ILogger logger, Properties properties) {
        Properties nonNullProps = properties == null ? new Properties() : properties;
        MemberAddressProvider provider = InstantiationUtils.newInstanceOrNull(clazz, nonNullProps, logger);
        if (provider == null) {
            provider = InstantiationUtils.newInstanceOrNull(clazz, logger, nonNullProps);
        }
        if (provider == null) {
            provider = InstantiationUtils.newInstanceOrNull(clazz, nonNullProps);
        }
        if (provider == null) {
            if (properties != null && !properties.isEmpty()) {
                throw new InvalidConfigurationException("Cannot find a matching constructor for MemberAddressProvider.  "
                        + "The member address provider has properties configured, but the class " + "'"
                        + clazz.getName() + "' does not have a public constructor accepting properties.");
            }
            provider = InstantiationUtils.newInstanceOrNull(clazz, logger);
        }
        if (provider == null) {
            provider = InstantiationUtils.newInstanceOrNull(clazz);
        }
        if (provider == null) {
            throw new InvalidConfigurationException("Cannot find a matching constructor for MemberAddressProvider "
                    + "implementation '" + clazz.getName() + "'.");
        }
        return provider;
    }

    private Class<? extends MemberAddressProvider> loadMemberAddressProviderClass(ClassLoader classLoader,
                                                                                  String classname) {
        try {
            Class<?> clazz = ClassLoaderUtil.loadClass(classLoader, classname);
            if (!(MemberAddressProvider.class.isAssignableFrom(clazz))) {
                throw new InvalidConfigurationException("Configured member address provider " + clazz.getName()
                        + " does not implement the interface" + MemberAddressProvider.class.getName());
            }
            return (Class<? extends MemberAddressProvider>) clazz;
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException("Cannot create a new instance of MemberAddressProvider '" + classname
                    + "'", e);
        }
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
        TcpServerContext context = new TcpServerContext(node, node.nodeEngine);
        Networking networking = createNetworking(node);
        Config config = node.getConfig();

        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        return new TcpServer(config,
                context,
                registry,
                addressRegistry,
                metricsRegistry,
                networking,
                node.getNodeExtension().createChannelInitializerFn(context));
    }

    private Networking createNetworking(Node node) {
        LoggingServiceImpl loggingService = node.loggingService;
        ILogger logger = loggingService.getLogger(TcpServerConnectionChannelErrorHandler.class);
        ChannelErrorHandler errorHandler = new TcpServerConnectionChannelErrorHandler(logger);
        HazelcastProperties props = node.getProperties();

        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(node.nodeEngine.getMetricsRegistry())
                        .threadNamePrefix(node.hazelcastInstance.getName())
                        .errorHandler(errorHandler)
                        .inputThreadCount(props.getInteger(IO_INPUT_THREAD_COUNT))
                        .inputThreadAffinity(newSystemThreadAffinity("hazelcast.io.input.thread.affinity"))
                        .outputThreadCount(props.getInteger(IO_OUTPUT_THREAD_COUNT))
                        .outputThreadAffinity(newSystemThreadAffinity("hazelcast.io.output.thread.affinity"))
                        .balancerIntervalSeconds(props.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .writeThroughEnabled(props.getBoolean(IO_WRITE_THROUGH_ENABLED))
                        .concurrencyDetection(node.nodeEngine.getConcurrencyDetection())
        );
    }
}
