/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import com.hazelcast.impl.Util;
import com.hazelcast.nio.Address;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XmlConfigBuilder implements ConfigBuilder {

    private final static Logger logger = Logger.getLogger(XmlConfigBuilder.class.getName());
    private boolean domLevel3 = true;
    private Config config;
    private InputStream in;
    private File configurationFile;
    private URL configurationUrl;
    boolean usingSystemConfig = false;

    public static class IterableNodeList implements Iterable<Node> {

    	private final NodeList parent;
    	private final int maximum;

    	public IterableNodeList(final NodeList parent) {
    		this.parent = parent;
    		this.maximum = parent.getLength();
    	}
    	
		public Iterator<Node> iterator() {
			return new Iterator<Node>() {
				
				private int index = 0;

				public boolean hasNext() {
					return (index < maximum);
				}

				public Node next() {
					return parent.item(index++);
				}

				public void remove() {
					throw new UnsupportedOperationException();
				}
				
			};
		}
    	
    }
    
    public XmlConfigBuilder(String xmlFileName) throws FileNotFoundException {
        this(new FileInputStream(xmlFileName));
    }

    public XmlConfigBuilder(InputStream inputStream) {
        this.in = inputStream;
    }

    public XmlConfigBuilder() {
        String configFile = System.getProperty("hazelcast.config");
        try {
            if (configFile != null) {
                configurationFile = new File(configFile);
                if (!configurationFile.exists()) {
                    String msg = "Config file at '" + configFile + "' doesn't exist.";
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the classpath.";
                    logger.log(Level.WARNING, msg);
                    configurationFile = null;
                }
            }
            if (configurationFile == null) {
                configFile = "hazelcast.xml";
                configurationFile = new File("hazelcast.xml");
                if (!configurationFile.exists()) {
                    configurationFile = null;
                }
            }
            if (configurationFile != null) {
                logger.log(Level.INFO, "Using configuration file at " + configurationFile.getAbsolutePath());
                try {
                    in = new FileInputStream(configurationFile);
                    configurationUrl = configurationFile.toURI().toURL();
                    usingSystemConfig = true;
                } catch (final Exception e) {
                    String msg = "Having problem reading config file at '" + configFile + "'.";
                    msg += "\nException message: " + e.getMessage();
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the jar.";
                    logger.log(Level.WARNING, msg);
                    in = null;
                }
            }
            if (in == null) {
                configurationUrl = Config.class.getClassLoader().getResource("hazelcast.xml");
                if (configurationUrl == null)
                    return;
                in = Config.class.getClassLoader().getResourceAsStream("hazelcast.xml");
                if (in == null) {
                    String msg = "Having problem reading config file hazelcast.xml in the classpath.";
                    msg += "\nHazelcast will start with default configuration.";
                    logger.log(Level.WARNING, msg);
                }
            }
        } catch (final Exception e) {
            logger.log(Level.SEVERE, "Error while creating configuration", e);
            e.printStackTrace();
        }
    }

    public Config build() {
        Config config = new Config();
        try {
            parse(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        config.setConfigurationFile(configurationFile);
        config.setConfigurationUrl(configurationUrl);
        return config;
    }

    private void parse(final Config config) throws Exception {
        this.config = config;
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc = null;
        try {
            doc = builder.parse(in);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Util.streamXML(doc, baos);
            final byte[] bytes = baos.toByteArray();
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            config.setXmlConfig(Util.inputStreamToString(bais));
            if ("true".equals(System.getProperty("hazelcast.config.print"))) {
                logger.log(Level.INFO, "Hazelcast config URL : " + config.getConfigurationUrl());
                logger.log(Level.INFO, "=== Hazelcast config xml ===");
                logger.log(Level.INFO, config.getXmlConfig());
                logger.log(Level.INFO, "==============================");
                logger.log(Level.INFO, "");
            }
        } catch (final Exception e) {
            String msgPart = "config file '" + config.getConfigurationFile() + "' set as a system property.";
            if (!usingSystemConfig) {
                msgPart = "hazelcast.xml config file in the classpath.";
            }
            String msg = "Having problem parsing the " + msgPart;
            msg += "\nException: " + e.getMessage();
            msg += "\nHazelcast will start with default configuration.";
            logger.log(Level.WARNING, msg);
            return;
        }
        final Element docElement = doc.getDocumentElement();
        try {
            docElement.getTextContent();
        } catch (final Throwable e) {
            domLevel3 = false;
        }
        
        for(org.w3c.dom.Node node  : new IterableNodeList(docElement.getChildNodes())) {
            final String nodeName = node.getNodeName();
            if ("network".equals(nodeName)) {
                handleNetwork(node);
            } else if ("group".equals(nodeName)) {
                handleGroup(node);
            } else if ("properties".equals(nodeName)) {
                handleProperties(node);
            } else if ("executor-service".equals(nodeName)) {
                handleExecutor(node);
            } else if ("queue".equals(nodeName)) {
                handleQueue(node);
            } else if ("map".equals(nodeName)) {
                handleMap(node);
            } else if ("topic".equals(nodeName)) {
                handleTopic(node);
            }
        }
    }

    private boolean checkTrue(final String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("on".equalsIgnoreCase(value)) {
            return true;
        }
        return false;
    }

    private void handleNetwork(final org.w3c.dom.Node node) throws Exception {
        for(org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = child.getNodeName();
            if ("port".equals(nodeName)) {
                handlePort(child);
            } else if ("join".equals(nodeName)) {
                handleJoin(child);
            } else if ("interfaces".equals(nodeName)) {
                handleInterfaces(child);
            } else if ("symmetric-encryption".equals(nodeName)) {
                handleViaReflection(child, config.getNetworkConfig(), new SymmetricEncryptionConfig());
            } else if ("asymmetric-encryption".equals(nodeName)) {
                handleViaReflection(child, config.getNetworkConfig(), new AsymmetricEncryptionConfig());
            }
        }
    }

    private int getIntegerValue(final String parameterName, final String value,
                                final int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (final Exception e) {
            logger.log(Level.INFO, parameterName + " parameter value, [" + value
                    + "], is not a proper integer. Default value, [" + defaultValue
                    + "], will be used!");
            e.printStackTrace();
            return defaultValue;
        }
    }

    private String getTextContent(final Node node) {
        if (domLevel3) {
            return node.getTextContent();
        } else {
            return getTextContent2(node);
        }
    }

    private String getTextContent2(final Node node) {
        final Node child = node.getFirstChild();
        if (child != null) {
            final Node next = child.getNextSibling();
            if (next == null) {
                return hasTextContent(child) ? child.getNodeValue() : "";
            }
            final StringBuffer buf = new StringBuffer();
            getTextContent2(node, buf);
            return buf.toString();
        }
        return "";
    }

    private void getTextContent2(final Node node, final StringBuffer buf) {
        Node child = node.getFirstChild();
        while (child != null) {
            if (hasTextContent(child)) {
                getTextContent2(child, buf);
            }
            child = child.getNextSibling();
        }
    }

    private void handleExecutor(final org.w3c.dom.Node node) {
        final ExecutorConfig executorConfig = config.getExecutorConfig();
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String name = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            if ("core-pool-size".equals(name)) {
                executorConfig.setCorePoolSize(getIntegerValue("core-pool-size", value, ExecutorConfig.DEFAULT_CORE_POOL_SIZE));
            } else if ("max-pool-size".equals(name)) {
                executorConfig.setMaxPoolSize(getIntegerValue("max-pool-size", value, ExecutorConfig.DEFAULT_MAX_POOL_SIZE));
            } else if ("keep-alive-seconds".equals(name)) {
                executorConfig.setKeepAliveSeconds(getIntegerValue("keep-alive-seconds", value, ExecutorConfig.DEFAULT_KEEPALIVE_SECONDS));
            }
        }
    }

    private void handleGroup(final org.w3c.dom.Node node) {
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String nodeName = n.getNodeName().toLowerCase();
            if ("name".equals(nodeName)) {
                config.setGroupName(value);
            } else if ("password".equals(nodeName)) {
                config.setGroupPassword(value);
            }
        }
    }

    private void handleProperties(final org.w3c.dom.Node node) {
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            final String name = n.getNodeName().toLowerCase();
            System.setProperty(name, value);
        }
    }

    private void handleInterfaces(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Interfaces interfaces = config.getNetworkConfig().getInterfaces();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            if ("enabled".equals(att.getNodeName())) {
                final String value = att.getNodeValue();
                interfaces.setEnabled(checkTrue(value));
            }
        }
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            if ("interface".equalsIgnoreCase(n.getNodeName())) {
                final String value = getTextContent(n).trim();
                interfaces.addInterface(value);
            }
        }
    }

    private void handleViaReflection(final org.w3c.dom.Node node, Object parent, Object target) throws Exception {
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            String methodName = "set" + getMethodName(att.getNodeName());
            Method method = getMethod(target, methodName);
            final String value = att.getNodeValue();
            invoke(target, method, value);
        }
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            String methodName = "set" + getMethodName(n.getNodeName());
            Method method = getMethod(target, methodName);
            invoke(target, method, value);
        }
        String mName = "set" + target.getClass().getSimpleName();
        Method method = getMethod(parent, mName);
        if (method == null) {
            mName = "add" + target.getClass().getSimpleName();
            method = getMethod(parent, mName);
        }
        method.invoke(parent, new Object[]{target});
    }

    private void invoke(Object target, Method method, String value) {
        if (method == null) return;
        Class<?>[] args = method.getParameterTypes();
        if (args == null || args.length == 0) return;
        Class<?> arg = method.getParameterTypes()[0];
        try {
            if (arg == String.class) {
                method.invoke(target, new Object[]{value});
            } else if (arg == int.class) {
                method.invoke(target, new Object[]{Integer.parseInt(value)});
            } else if (arg == long.class) {
                method.invoke(target, new Object[]{Long.parseLong(value)});
            } else if (arg == boolean.class) {
                method.invoke(target, new Object[]{Boolean.parseBoolean(value)});
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Method getMethod(Object target, String methodName) {
        Method[] methods = target.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equalsIgnoreCase(methodName)) {
                return method;
            }
        }
        return null;
    }

    private String getMethodName(String element) {
        StringBuilder sb = new StringBuilder();
        char[] chars = element.toCharArray();
        boolean upper = true;
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (c == '_' || c == '-' || c == '.') {
                upper = true;
            } else {
                if (upper) {
                    sb.append(Character.toUpperCase(c));
                    upper = false;
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    private void handleJoin(final org.w3c.dom.Node node) {
        for(org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = child.getNodeName().toLowerCase();
            if ("multicast".equals(name)) {
                handleMulticast(child);
            } else if ("tcp-ip".equals(name)) {
                handleTcpIp(child);
            }
        }
    }

    private void handleMulticast(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Join join = config.getNetworkConfig().getJoin();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if ("enabled".equalsIgnoreCase(att.getNodeName())) {
                join.getMulticastConfig().setEnabled(checkTrue(value));
            }
        }
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            if ("multicast-group".equalsIgnoreCase(n.getNodeName())) {
                join.getMulticastConfig().setMulticastGroup(value);
            } else if ("multicast-port".equalsIgnoreCase(n.getNodeName())) {
                join.getMulticastConfig().setMulticastPort(Integer.parseInt(value));
            }
        }
    }

    private void handlePort(final org.w3c.dom.Node node) {
        final String portStr = getTextContent(node).trim();
        if (portStr != null && portStr.length() > 0) {
            config.setPort(Integer.parseInt(portStr));
        }
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("auto-increment")) {
                config.setPortAutoIncrement(checkTrue(value));
            }
        }
    }

    private void handleQueue(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final QueueConfig qConfig = new QueueConfig();
        qConfig.setName(name);
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            if ("max-size-per-jvm".equals(nodeName)) {
                qConfig.setMaxSizePerJVM(getIntegerValue("max-size-per-jvm", value, QueueConfig.DEFAULT_MAX_SIZE_PER_JVM));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                qConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value, QueueConfig.DEFAULT_TTL_SECONDS));
            }
        }
        this.config.getMapQConfigs().put(name, qConfig);
    }

    private void handleMap(final org.w3c.dom.Node node) throws Exception{
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName(name);
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            if ("backup-count".equals(nodeName)) {
                mapConfig.setBackupCount(getIntegerValue("backup-count", value, MapConfig.DEFAULT_BACKUP_COUNT));
            } else if ("eviction-policy".equals(nodeName)) {
                mapConfig.setEvictionPolicy(value);
            } else if ("max-size".equals(nodeName)) {
                mapConfig.setMaxSize(getIntegerValue("max-size", value,
                        MapConfig.DEFAULT_MAX_SIZE));
            } else if ("eviction-percentage".equals(nodeName)) {
                mapConfig.setEvictionPercentage(getIntegerValue("eviction-percentage", value,
                        MapConfig.DEFAULT_EVICTION_PERCENTAGE));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                mapConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value,
                        MapConfig.DEFAULT_TTL_SECONDS));
            } else if ("map-store".equals(nodeName)) {
                MapStoreConfig mapStoreConfig = createMapStoreConfig(n);
                mapConfig.setMapStoreConfig(mapStoreConfig);
            } else if ("near-cache".equals(nodeName)) {
                handleViaReflection(n, mapConfig, new NearCacheConfig());
            }
        }
        this.config.getMapMapConfigs().put(name, mapConfig);
    }

    private MapStoreConfig createMapStoreConfig(final org.w3c.dom.Node node) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                mapStoreConfig.setEnabled(checkTrue(value));
            }
        }
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            if ("class-name".equals(nodeName)) {
                mapStoreConfig.setClassName(value);
            } else if ("write-delay-seconds".equals(nodeName)) {
                mapStoreConfig.setWriteDelaySeconds(getIntegerValue("write-delay-seconds", value, MapStoreConfig.DEFAULT_WRITE_DELAY_SECONDS));
            }
        }
        return mapStoreConfig;
    }

    private void handleTcpIp(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Join join = config.getNetworkConfig().getJoin();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                join.getJoinMembers().setEnabled(checkTrue(value));
            } else if (att.getNodeName().equals("conn-timeout-seconds")) {
                join.getJoinMembers().setConnectionTimeoutSeconds(getIntegerValue("conn-timeout-seconds", value, 5));
            }
        }
        final NodeList nodelist = node.getChildNodes();
        members:
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("required-member")) {
                join.getJoinMembers().setRequiredMember(value);
            } else if (n.getNodeName().equalsIgnoreCase("hostname")) {
                join.getJoinMembers().addMember(value);
            } else if (n.getNodeName().equalsIgnoreCase("address")) {
                int colonIndex = value.indexOf(':');
                if (colonIndex == -1) {
                    logger.log(Level.WARNING, "Address should be in the form of ip:port. Address [" + value + "] is not valid.");
                } else {
                    String hostStr = value.substring(0, colonIndex);
                    String portStr = value.substring(colonIndex + 1);
                    try {
                        join.getJoinMembers().addAddress(new Address(hostStr, Integer.parseInt(portStr), true));
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            } else if ("interface".equalsIgnoreCase(n.getNodeName())) {
                final int indexStar = value.indexOf('*');
                final int indexDash = value.indexOf('-');
                if (indexStar == -1 && indexDash == -1) {
                    join.getJoinMembers().addMember(value);
                } else {
                    final String first3 = value.substring(0, value.lastIndexOf('.'));
                    final String lastOne = value.substring(value.lastIndexOf('.') + 1);
                    if (first3.indexOf('*') != -1 && first3.indexOf('-') != -1) {
                        String msg = "First 3 parts of interface definition cannot contain '*' and '-'.";
                        msg += "\nPlease change the value '" + value + "' in the config file.";
                        logger.log(Level.WARNING, msg);
                        continue members;
                    }
                    if (lastOne.equals("*")) {
                        for (int j = 0; j < 256; j++) {
                            join.getJoinMembers().addMember(first3 + "." + String.valueOf(j));
                        }
                    } else if (lastOne.indexOf('-') != -1) {
                        final int start = Integer.parseInt(lastOne.substring(0, lastOne
                                .indexOf('-')));
                        final int end = Integer.parseInt(lastOne
                                .substring(lastOne.indexOf('-') + 1));
                        for (int j = start; j <= end; j++) {
                            join.getJoinMembers().addMember(first3 + "." + String.valueOf(j));
                        }
                    }
                }
            }
        }
    }

    private void handleTopic(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final TopicConfig tConfig = new TopicConfig();
        tConfig.setName(name);
        
        for(org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("global-ordering-enabled")) {
                tConfig.setGlobalOrderingEnabled(checkTrue(value));
            }
        }
        config.getMapTopicConfigs().put(name, tConfig);
    }

    private boolean hasTextContent(final Node child) {
        final boolean result = child.getNodeType() != Node.COMMENT_NODE
                && child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE;
        return result;
    }
}
