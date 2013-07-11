/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.util.ExceptionUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteOrder;
import java.util.EventListener;
import java.util.logging.Level;

public class XmlClientConfigBuilder extends AbstractXmlConfigHelper {

    private final static ILogger logger = Logger.getLogger(XmlClientConfigBuilder.class);

    private ClientConfig clientConfig;
    private InputStream in;

    public XmlClientConfigBuilder(String resource) throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        if (url == null) {
            throw new IllegalArgumentException("Could not load " + resource);
        }
        this.in = url.openStream();
    }

    public XmlClientConfigBuilder(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File is null!");
        }
        in = new FileInputStream(file);
    }

    public XmlClientConfigBuilder(URL url) throws IOException {
        if (url == null) {
            throw new NullPointerException("URL is null!");
        }
        in = url.openStream();
    }

    public XmlClientConfigBuilder(InputStream in) {
        this.in = in;
    }

    public XmlClientConfigBuilder() {
        String configFile = System.getProperty("hazelcast.client.config");
        try {
            File configurationFile = null;
            if (configFile != null) {
                configurationFile = new File(configFile);
                logger.log(Level.INFO, "Using configuration file at " + configurationFile.getAbsolutePath());
                if (!configurationFile.exists()) {
                    String msg = "Config file at '" + configurationFile.getAbsolutePath() + "' doesn't exist.";
                    msg += "\nHazelcast will try to use the hazelcast-client.xml config file in the working directory.";
                    logger.log(Level.WARNING, msg);
                    configurationFile = null;
                }
            }
            if (configurationFile == null) {
                configFile = "hazelcast-client-default.xml";
                configurationFile = new File("hazelcast-client-default.xml");
                if (!configurationFile.exists()) {
                    configurationFile = null;
                }
            }
            URL configurationUrl;
            if (configurationFile != null) {
                logger.log(Level.INFO, "Using configuration file at " + configurationFile.getAbsolutePath());
                try {
                    in = new FileInputStream(configurationFile);
                    configurationUrl = configurationFile.toURI().toURL();
                } catch (final Exception e) {
                    String msg = "Having problem reading config file at '" + configFile + "'.";
                    msg += "\nException message: " + e.getMessage();
                    msg += "\nHazelcast will try to use the hazelcast-client.xml config file in classpath.";
                    logger.log(Level.WARNING, msg);
                    in = null;
                }
            }
            if (in == null) {
                logger.log(Level.INFO, "Looking for hazelcast-client.xml config file in classpath.");
                configurationUrl = Config.class.getClassLoader().getResource("hazelcast-client-default.xml");
                if (configurationUrl == null) {
                    throw new IllegalStateException("Cannot find hazelcast-client.xml in classpath, giving up.");
                }
                logger.log(Level.INFO, "Using configuration file " + configurationUrl.getFile() + " in the classpath.");
                in = configurationUrl.openStream();
                if (in == null) {
                    throw new IllegalStateException("Cannot read configuration file, giving up.");
                }
            }
        } catch (final Throwable e) {
            logger.log(Level.SEVERE, "Error while creating configuration:" + e.getMessage(), e);
        }
    }

    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClassLoader(classLoader);
        try {
            parse(clientConfig);
            return clientConfig;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void parse(ClientConfig clientConfig) throws Exception {
        this.clientConfig = clientConfig;
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc;
        try {
            doc = builder.parse(in);
        } catch (final Exception e) {
            throw new IllegalStateException("Could not parse configuration file, giving up.");
        }
        Element element = doc.getDocumentElement();
        try {
            element.getTextContent();
        } catch (final Throwable e) {
            domLevel3 = false;
        }
        handleConfig(element);
    }

    private void handleConfig(final Element docElement) throws Exception {
        for (Node node : new IterableNodeList(docElement.getChildNodes())) {
            final String nodeName = cleanNodeName(node.getNodeName());
            if ("security".equals(nodeName)) {
                handleSecurity(node);
            } else if ("socket-interceptor".equals(nodeName)) {
                handleSocketInterceptor(node);
            } else if ("proxy-factories".equals(nodeName)) {
                handleProxyFactories(node);
            } else if ("serialization".equals(nodeName)) {
                handleSerialization(node);
            } else if ("group".equals(nodeName)) {
                handleGroup(node);
            } else if ("listeners".equals(nodeName)) {
                handleListeners(node);
            } else if ("network".equals(nodeName)) {
                handleNetwork(node);
            } else if ("load-balancer".equals(nodeName)) {
                handleLoadBalancer(node);
            }
        }
    }

    private void handleLoadBalancer(Node node) {
        final String type = getAttribute(node, "type");
        if ("random".equals(type)) {
            clientConfig.setLoadBalancer(new RandomLB());
        } else if ("round-robin".equals(type)) {
            clientConfig.setLoadBalancer(new RoundRobinLB());
        }
    }

    private void handleNetwork(Node node) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child);
            if ("cluster-members".equals(nodeName)) {
                handleClusterMembers(child);
            } else if ("smart-routing".equals(nodeName)) {
                clientConfig.setSmart(Boolean.parseBoolean(getTextContent(child)));
            } else if ("redo-operation".equals(nodeName)) {
                clientConfig.setRedoOperation(Boolean.parseBoolean(getTextContent(child)));
            } else if ("connection-pool-size".equals(nodeName)) {
                clientConfig.setConnectionPoolSize(Integer.parseInt(getTextContent(child)));
            } else if ("connection-timeout".equals(nodeName)) {
                clientConfig.setConnectionTimeout(Integer.parseInt(getTextContent(child)));
            } else if ("connection-attempt-period".equals(nodeName)) {
                clientConfig.setConnectionAttemptPeriod(Integer.parseInt(getTextContent(child)));
            } else if ("connection-attempt-limit".equals(nodeName)) {
                clientConfig.setConnectionAttemptLimit(Integer.parseInt(getTextContent(child)));
            } else if ("socket-options".equals(nodeName)) {
                handleSocketOptions(child);
            }
        }
    }

    private void handleSocketOptions(Node node) {
        SocketOptions socketOptions = clientConfig.getSocketOptions();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child);
            if ("tcp-no-delay".equals(nodeName)) {
                socketOptions.setSocketTcpNoDelay(Boolean.parseBoolean(getTextContent(child)));
            } else if ("keep-alive".equals(nodeName)) {
                socketOptions.setSocketKeepAlive(Boolean.parseBoolean(getTextContent(child)));
            } else if ("reuse-address".equals(nodeName)) {
                socketOptions.setSocketReuseAddress(Boolean.parseBoolean(getTextContent(child)));
            } else if ("linger-seconds".equals(nodeName)) {
                socketOptions.setSocketLingerSeconds(Integer.parseInt(getTextContent(child)));
            } else if ("timeout".equals(nodeName)) {
                socketOptions.setSocketTimeout(Integer.parseInt(getTextContent(child)));
            } else if ("buffer-size".equals(nodeName)) {
                socketOptions.setSocketBufferSize(Integer.parseInt(getTextContent(child)));
            }
        }
    }

    private void handleClusterMembers(Node node) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            if ("address".equals(cleanNodeName(child))) {
                clientConfig.addAddress(getTextContent(child));
            }
        }
    }

    private void handleListeners(Node node) throws Exception {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            if ("listener".equals(cleanNodeName(child))) {
                String className = getTextContent(child);
                handleEventListenerInstantiation(className);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleEventListenerInstantiation(String className) throws Exception {
        ClassLoader classLoader = clientConfig.getClassLoader();
        Class<? extends EventListener> listenerClass;
        listenerClass = (Class<? extends EventListener>) classLoader.loadClass(className);
        EventListener listener = listenerClass.newInstance();
        clientConfig.getListeners().add(listener);
    }

    private void handleGroup(Node node) {
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("name".equals(nodeName)) {
                clientConfig.getGroupConfig().setName(value);
            } else if ("password".equals(nodeName)) {
                clientConfig.getGroupConfig().setPassword(value);
            }
        }
    }

    private void handleSerialization(Node node) {
        SerializationConfig serializationConfig = clientConfig.getSerializationConfig();
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("portable-version".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setPortableVersion(getIntegerValue(name, value, 0));
            } else if ("check-class-def-errors".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setCheckClassDefErrors(checkTrue(value));
            } else if ("use-native-byte-order".equals(name)) {
                serializationConfig.setUseNativeByteOrder(checkTrue(getTextContent(child)));
            } else if ("byte-order".equals(name)) {
                String value = getTextContent(child);
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
                serializationConfig.setByteOrder(byteOrder != null ? byteOrder : ByteOrder.BIG_ENDIAN);
            } else if ("enable-compression".equals(name)) {
                serializationConfig.setEnableCompression(checkTrue(getTextContent(child)));
            } else if ("enable-shared-object".equals(name)) {
                serializationConfig.setEnableSharedObject(checkTrue(getTextContent(child)));
            } else if ("allow-unsafe".equals(name)) {
                serializationConfig.setAllowUnsafe(checkTrue(getTextContent(child)));
            } else if ("data-serializable-factories".equals(name)) {
                handleDataSerializableFactories(child, serializationConfig);
            } else if ("portable-factories".equals(name)) {
                handlePortableFactories(child, serializationConfig);
            } else if ("serializers".equals(name)) {
                handleSerializers(child, serializationConfig);
            }
        }
    }

    private void handleDataSerializableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("data-serializable-factory".equals(name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = child.getAttributes().getNamedItem("factory-id");
                if (factoryIdNode == null) {
                    throw new IllegalArgumentException(
                            "'factory-id' attribute of 'data-serializable-factory' is required!");
                }
                int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
                serializationConfig.addDataSerializableFactoryClass(factoryId, value);
            }
        }
    }

    private void handlePortableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("portable-factory".equals(name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = child.getAttributes().getNamedItem("factory-id");
                if (factoryIdNode == null) {
                    throw new IllegalArgumentException("'factory-id' attribute of 'portable-factory' is required!");
                }
                int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
                serializationConfig.addPortableFactoryClass(factoryId, value);
            }
        }
    }

    private void handleSerializers(Node node, SerializationConfig serializationConfig) {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            final String value = getTextContent(child);
            if ("serializer".equals(name)) {
                SerializerConfig serializerConfig = new SerializerConfig();
                serializerConfig.setClassName(value);
                final String typeClassName = getAttribute(child, "type-class");
                serializerConfig.setTypeClassName(typeClassName);
                serializationConfig.addSerializerConfig(serializerConfig);
            } else if ("global-serializer".equals(name)) {
                GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
                globalSerializerConfig.setClassName(value);
                serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
            }
        }
    }

    private void handleProxyFactories(Node node) throws Exception {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("proxy-factory".equals(nodeName)) {
                handleProxyFactory(child);
            }
        }
    }

    private void handleProxyFactory(Node node) throws Exception {
        final String service = getAttribute(node, "service");
        final String className = getAttribute(node, "class-name");
        handleProxyFactoryInstantiation(service, className);
    }

    @SuppressWarnings("unchecked")
    private void handleProxyFactoryInstantiation(String service, String className) throws Exception {
        ClassLoader classLoader = clientConfig.getClassLoader();
        Class<? extends ClientProxyFactory> proxyFactoryClass;
        proxyFactoryClass = (Class<? extends ClientProxyFactory>) classLoader.loadClass(className);
        ClientProxyFactory factory = proxyFactoryClass.newInstance();
        clientConfig.getProxyFactoryConfig().addProxyFactory(service, factory);
    }

    private void handleSocketInterceptor(Node node) throws Exception {
        String className = getAttribute(node, "class-name");
        handleSocketInterceptorInstantiation(className);
    }

    @SuppressWarnings("unchecked")
    private void handleSocketInterceptorInstantiation(String className) throws Exception {
        ClassLoader classLoader = clientConfig.getClassLoader();
        Class<? extends SocketInterceptor> socketInterceptorClass;
        socketInterceptorClass = (Class<? extends SocketInterceptor>) classLoader.loadClass(className);
        SocketInterceptor interceptor = socketInterceptorClass.newInstance();
        clientConfig.setSocketInterceptor(interceptor);
    }

    private void handleSecurity(Node node) throws Exception {
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("login-credentials".equals(nodeName)) {
                handleLoginCredentials(child);
            }
        }
    }

    private void handleLoginCredentials(Node node) {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials();
        for (Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(child.getNodeName());
            if ("username".equals(nodeName)) {
                credentials.setUsername(getTextContent(child));
            } else if ("password".equals(nodeName)) {
                credentials.setPassword(getTextContent(child));
            }
        }
        clientConfig.setCredentials(credentials);
    }

    private String getAttribute(Node node, String attName) {
        final Node attNode = node.getAttributes().getNamedItem(attName);
        if (attNode == null)
            return null;
        return getTextContent(attNode);
    }

    private int getIntegerValue(String parameterName, String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (final Exception e) {
            logger.log(Level.INFO, parameterName + " parameter value, [" + value
                    + "], is not a proper integer. Default value, [" + defaultValue + "], will be used!");
            logger.log(Level.WARNING, e.getMessage(), e);
            return defaultValue;
        }
    }
}
