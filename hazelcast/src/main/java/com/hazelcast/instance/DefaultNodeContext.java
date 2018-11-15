/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.cluster.Joiner;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.nio.NioNetworking;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.TcpIpConnectionChannelErrorHandler;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.MemberAddressProvider;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.spi.properties.GroupProperty.IO_BALANCER_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.IO_OUTPUT_THREAD_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

@PrivateApi
public class DefaultNodeContext implements NodeContext {

    public static final List<String> EXTENSION_PRIORITY_LIST = unmodifiableList(asList(
            "com.hazelcast.instance.EnterpriseNodeExtension",
            "com.hazelcast.instance.DefaultNodeExtension"
    ));

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node, EXTENSION_PRIORITY_LIST);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        Config config = node.getConfig();
        MemberAddressProviderConfig memberAddressProviderConfig = config.getNetworkConfig().getMemberAddressProviderConfig();

        final ILogger addressPickerLogger = node.getLogger(AddressPicker.class);
        if (!memberAddressProviderConfig.isEnabled()) {
            return new DefaultAddressPicker(config, addressPickerLogger);
        }

        MemberAddressProvider implementation = memberAddressProviderConfig.getImplementation();
        if (implementation != null) {
            return new DelegatingAddressPicker(implementation, config.getNetworkConfig(), addressPickerLogger);
        }
        ClassLoader classLoader = config.getClassLoader();
        String classname = memberAddressProviderConfig.getClassName();
        Class<?> clazz = loadMemberAddressProviderClass(classLoader, classname);

        Constructor<?> constructor = findMemberAddressProviderConstructor(clazz);
        Properties properties = memberAddressProviderConfig.getProperties();
        MemberAddressProvider memberAddressProvider = newMemberAddressProviderInstance(constructor, properties);
        return new DelegatingAddressPicker(memberAddressProvider, config.getNetworkConfig(), addressPickerLogger);

    }

    private MemberAddressProvider newMemberAddressProviderInstance(Constructor<?> constructor, Properties properties) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        Class<?> clazz = constructor.getDeclaringClass();
        String classname = clazz.getName();
        try {
            if (parameterTypes.length == 0) {
                //we have only the no-arg constructor -> we have to fail-fast when some properties were configured
                if (properties != null && !properties.isEmpty()) {
                    throw new ConfigurationException("Cannot find a matching constructor for MemberAddressProvider.  "
                            + "The member address provider has properties configured, but the class " + "'" + classname
                            + "' does not have a public constructor accepting properties.");
                }

                return (MemberAddressProvider) constructor.newInstance();
            } else {
                if (properties == null) {
                    properties = new Properties();
                }
                return (MemberAddressProvider) constructor.newInstance(properties);
            }
        } catch (InstantiationException e) {
            throw new ConfigurationException("Cannot create a new instance of MemberAddressProvider '" + clazz + "'", e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Cannot create a new instance of MemberAddressProvider '" + clazz + "'", e);
        } catch (InvocationTargetException e) {
            throw new ConfigurationException("Cannot create a new instance of MemberAddressProvider '" + clazz + "'", e);
        }
    }

    private Constructor<?> findMemberAddressProviderConstructor(Class<?> clazz) {
        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(Properties.class);
        } catch (NoSuchMethodException e) {
            try {
                constructor = clazz.getConstructor();
            } catch (NoSuchMethodException e1) {
                throw new ConfigurationException("Cannot create a new instance of MemberAddressProvider '" + clazz + "'", e);
            }
        }
        return constructor;
    }

    private Class<?> loadMemberAddressProviderClass(ClassLoader classLoader, String classname) {
        try {
            return ClassLoaderUtil.loadClass(classLoader, classname);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Cannot create a new instance of MemberAddressProvider '" + classname + "'", e);
        }
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        Networking networking = createNetworking(node, ioService);

        return new TcpIpConnectionManager(
                ioService,
                serverSocketChannel,
                node.loggingService,
                node.nodeEngine.getMetricsRegistry(),
                networking,
                node.getProperties());
    }

    private Networking createNetworking(Node node, NodeIOService ioService) {
        ChannelInitializer initializer = node.getNodeExtension().createChannelInitializer(ioService);

        LoggingServiceImpl loggingService = node.loggingService;

        ChannelErrorHandler errorHandler
                = new TcpIpConnectionChannelErrorHandler(loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class));

        HazelcastProperties props = node.getProperties();

        return new NioNetworking(
                new NioNetworking.Context()
                        .loggingService(loggingService)
                        .metricsRegistry(node.nodeEngine.getMetricsRegistry())
                        .threadNamePrefix(node.hazelcastInstance.getName())
                        .errorHandler(errorHandler)
                        .inputThreadCount(props.getInteger(IO_INPUT_THREAD_COUNT))
                        .outputThreadCount(props.getInteger(IO_OUTPUT_THREAD_COUNT))
                        .balancerIntervalSeconds(props.getInteger(IO_BALANCER_INTERVAL_SECONDS))
                        .channelInitializer(initializer));
    }
}
