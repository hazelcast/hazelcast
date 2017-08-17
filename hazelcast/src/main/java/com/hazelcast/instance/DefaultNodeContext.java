/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.AddressLocatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.networking.EventLoopGroup;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.spinning.SpinningEventLoopGroup;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.MemberChannelInitializer;
import com.hazelcast.nio.tcp.TcpIpConnectionChannelErrorHandler;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.AddressLocator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.ServerSocketChannel;
import java.util.Properties;

@PrivateApi
public class DefaultNodeContext implements NodeContext {

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return NodeExtensionFactory.create(node);
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        Config config = node.getConfig();
        AddressLocatorConfig addressLocatorConfig = config.getNetworkConfig().getAddressLocatorConfig();

        if (!addressLocatorConfig.isEnabled()) {
            return new DefaultAddressPicker(config, node.getProperties(), node.getLogger(AddressPicker.class));
        }

        AddressLocator implementation = addressLocatorConfig.getImplementation();
        if (implementation != null) {
            return new DelegatingAddressPicker(implementation, config.getNetworkConfig());
        }
        ClassLoader classLoader = config.getClassLoader();
        String classname = addressLocatorConfig.getClassname();
        Class<?> clazz = loadAddressLocatorClass(classLoader, classname);

        Constructor<?> constructor = findAddressLocatorConstructor(clazz);
        Properties properties = addressLocatorConfig.getProperties();
        AddressLocator addressLocator = newAddressLocatorInstance(constructor, properties);
        return new DelegatingAddressPicker(addressLocator, config.getNetworkConfig());

    }

    private AddressLocator newAddressLocatorInstance(Constructor<?> constructor, Properties properties) {
        Class<?>[] parameterTypes = constructor.getParameterTypes();
        Class<?> clazz = constructor.getDeclaringClass();
        String classname = clazz.getName();
        try {
            if (parameterTypes.length == 0) {
                //we have only the no-arg constructor -> we have to fail-fast when some properties were configured
                if (properties != null && !properties.isEmpty()) {
                    throw new IllegalStateException("Cannot find a matching constructor for AddressLocator.  " +
                            "The address locator has properties configured, but the class " + "'" + classname + "' " +
                            "does not have a public constructor accepting properties.");
                }

                return (AddressLocator) constructor.newInstance();
            } else {
                if (properties == null) {
                    properties = new Properties();
                }
                return (AddressLocator) constructor.newInstance(properties);
            }
        } catch (InstantiationException e) {
            throw new IllegalStateException("Cannot create a new instance of AddressLocator '" + clazz + "'", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot create a new instance of AddressLocator '" + clazz + "'", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Cannot create a new instance of AddressLocator '" + clazz + "'", e);
        }
    }

    private Constructor<?> findAddressLocatorConstructor(Class<?> clazz) {
        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(Properties.class);
        } catch (NoSuchMethodException e) {
            try {
                constructor = clazz.getConstructor();
            } catch (NoSuchMethodException e1) {
                throw new IllegalStateException("Cannot create a new instance of AddressLocator '" + clazz + "'", e);
            }
        }
        return constructor;
    }

    private Class<?> loadAddressLocatorClass(ClassLoader classLoader, String classname) {
        try {
            return ClassLoaderUtil.loadClass(classLoader, classname);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot create a new instance of AddressLocator '" + classname +"'", e);
        }
    }

    @Override
    public Joiner createJoiner(Node node) {
        return node.createJoiner();
    }

    @Override
    public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
        NodeIOService ioService = new NodeIOService(node, node.nodeEngine);
        EventLoopGroup eventLoopGroup = createEventLoopGroup(node, ioService);

        return new TcpIpConnectionManager(
                ioService,
                serverSocketChannel,
                node.loggingService,
                node.nodeEngine.getMetricsRegistry(),
                eventLoopGroup);
    }

    private EventLoopGroup createEventLoopGroup(Node node, NodeIOService ioService) {
        boolean spinning = Boolean.getBoolean("hazelcast.io.spinning");
        LoggingServiceImpl loggingService = node.loggingService;

        MemberChannelInitializer initializer
                = new MemberChannelInitializer(loggingService.getLogger(MemberChannelInitializer.class), ioService);

        ChannelErrorHandler exceptionHandler
                = new TcpIpConnectionChannelErrorHandler(loggingService.getLogger(TcpIpConnectionChannelErrorHandler.class));

        if (spinning) {
            return new SpinningEventLoopGroup(
                    loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    exceptionHandler,
                    initializer,
                    node.hazelcastInstance.getName());
        } else {
            return new NioEventLoopGroup(
                    loggingService,
                    node.nodeEngine.getMetricsRegistry(),
                    node.hazelcastInstance.getName(),
                    exceptionHandler,
                    ioService.getInputSelectorThreadCount(),
                    ioService.getOutputSelectorThreadCount(),
                    ioService.getBalancerIntervalSeconds(),
                    initializer);
        }
    }

}
