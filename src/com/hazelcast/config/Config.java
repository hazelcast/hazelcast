/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.hazelcast.impl.Util;

public class Config {

	public static final int DEFAULT_PORT = 5701;

	public static final String DEFAULT_GROUP_PASSWORD = "group-pass";

	public static final String DEFAULT_GROUP_NAME = "group-dev";

	private final static Logger logger = Logger.getLogger(Config.class.getName());

    private static Config instance = new Config();

    private boolean domLevel3 = true;

    private URL urlConfig = null;

    private String xmlConfig = null;

    private String groupName = DEFAULT_GROUP_NAME;

    private String groupPassword = DEFAULT_GROUP_PASSWORD;

    private int port = DEFAULT_PORT;

    private boolean portAutoIncrement = true;

    private Interfaces interfaces = new Interfaces();

    private Join join = new Join();

    private ExecutorConfig executorConfig = new ExecutorConfig();

    private Map<String, TopicConfig> mapTopicConfigs = new HashMap<String, TopicConfig>();

    private Map<String, QueueConfig> mapQueueConfigs = new HashMap<String, QueueConfig>();

    private Map<String, MapConfig> mapMapConfigs = new HashMap<String, MapConfig>();

    private Config() {
        boolean usingSystemConfig = false;
        String configFile = System.getProperty("hazelcast.config");
        File fileConfig = null;
        InputStream in = null;
        try {
            if (configFile != null) {
                fileConfig = new File(configFile);
                if (!fileConfig.exists()) {
                    String msg = "Config file at '" + configFile + "' doesn't exist.";
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the classpath.";
                    logger.log(Level.WARNING, msg);
                    fileConfig = null;
                }
            }

            if (fileConfig == null) {
                configFile = "hazelcast.xml";
                fileConfig = new File("hazelcast.xml");
                if (!fileConfig.exists()) {
                    fileConfig = null;
                }
            }

            if (fileConfig != null) {
                logger.log (Level.INFO, "Using configuration file at " + fileConfig.getAbsolutePath());
                try {
                    in = new FileInputStream(fileConfig);
                    urlConfig = fileConfig.toURI().toURL();
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
                urlConfig = Config.class.getClassLoader().getResource("hazelcast.xml");
                if (urlConfig == null)
                    return;
                in = Config.class.getClassLoader().getResourceAsStream("hazelcast.xml");
                if (in == null) {
                    String msg = "Having problem reading config file hazelcast.xml in the classpath.";
                    msg += "\nHazelcast will start with default configuration.";
                    logger.log(Level.WARNING, msg);
                }
            }
            
            if (in == null) {
                return;
            }

            final DocumentBuilder builder = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder();
            Document doc = null;
            try {
                doc = builder.parse(in);
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Util.streamXML(doc, baos);
                final byte[] bytes = baos.toByteArray();
                final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                xmlConfig = Util.inputStreamToString(bais);
                if ("true".equals(System.getProperty("hazelcast.config.print"))) {
                    logger.log(Level.INFO, "Hazelcast config URL : " + urlConfig);
                    logger.log(Level.INFO, "=== Hazelcast config xml ===");
                    logger.log(Level.INFO, xmlConfig);
                    logger.log(Level.INFO, "==============================");
                    logger.log(Level.INFO, "");
                }
            } catch (final Exception e) {
                String msgPart = "config file '" + configFile + "' set as a system property.";
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
            final NodeList nodelist = docElement.getChildNodes();
            for (int i = 0; i < nodelist.getLength(); i++) {
                final org.w3c.dom.Node node = nodelist.item(i);
                final String nodeName = node.getNodeName();
                
                if ("network".equals(nodeName)) {
                    handleNetwork(node);
                } else if ("group".equals(nodeName)) {
                    handleGroup(node);
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
        } catch (final Exception e) {
        	logger.log(Level.SEVERE, "Error while creating configuration", e);
        	e.printStackTrace();
        }
    }

    public static Config get() {
        return instance;
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

    public QueueConfig getQConfig(final String name) {
        final Set<String> qNames = mapQueueConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapQueueConfigs.get(pattern);
            }
        }
        QueueConfig defaultConfig = mapQueueConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new QueueConfig();
        }
        return defaultConfig;
    }

    public MapConfig getMapConfig(final String name) {
        final Set<String> qNames = mapMapConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapMapConfigs.get(pattern);
            }
        }
        MapConfig defaultConfig = mapMapConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new MapConfig();
        }
        return defaultConfig;
    }

    public TopicConfig getTopicConfig(final String name) {
        final Set<String> tNames = mapTopicConfigs.keySet();
        for (final String pattern : tNames) {
            if (nameMatches(name, pattern)) {
                return mapTopicConfigs.get(pattern);
            }
        }
        TopicConfig defaultConfig = mapTopicConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new TopicConfig();
        }
        return defaultConfig;
    }

    private void handleNetwork(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node child = nodelist.item(i);
            final String nodeName = child.getNodeName();
            
            if ("port".equals(nodeName)) {
                handlePort(child);
            } else if ("join".equals(nodeName)) {
                handleJoin(child);
            } else if ("interfaces".equals(nodeName)) {
                handleInterfaces(child);
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
        final NodeList nodelist = node.getChildNodes();

        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String name = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            
            if ("core-pool-size".equals(name)) {
                executorConfig.setCorePoolSize(getIntegerValue("core-pool-size", value, ExecutorConfig.DEFAULT_CORE_POOL_SIZE));
            } else if ("max-pool-size".equals(name)) {
                executorConfig.setMaxPoolsize(getIntegerValue("max-pool-size", value, ExecutorConfig.DEFAULT_MAX_POOL_SIZE));
            } else if ("keep-alive-seconds".equals(name)) {
                executorConfig.setKeepAliveSeconds(getIntegerValue("keep-alive-seconds", value, ExecutorConfig.DEFAULT_KEEPALIVE_SECONDS));
            }
        }
    }

    private void handleGroup(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            final String nodeName = n.getNodeName().toLowerCase();
            if ("name".equals(nodeName)) {
                groupName = value;
            } else if ("password".equals(nodeName)) {
                groupPassword = value;
            }
        }
    }

    private void handleInterfaces(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = att.getNodeValue();
            if ("enabled".equals(att.getNodeName())) {
                interfaces.enabled = checkTrue(value);
            }
        }
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if ("interface".equalsIgnoreCase(n.getNodeName())) {
                interfaces.lsInterfaces.add(value);
            }
        }
    }

    private void handleJoin(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node child = nodelist.item(i);
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
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                join.multicastConfig.setEnabled(checkTrue(value));
            }
        }
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("multicast-group")) {
                join.multicastConfig.setMulticastGroup(value);
            } else if (n.getNodeName().equalsIgnoreCase("multicast-port")) {
                join.multicastConfig.setMulticastPort(Integer.parseInt(value));
            }
        }
    }

    private void handlePort(final org.w3c.dom.Node node) {
        final String portStr = getTextContent(node).trim();
        if (portStr != null && portStr.length() > 0) {
            port = Integer.parseInt(portStr);
        }
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("auto-increment")) {
                portAutoIncrement = checkTrue(value);
            }
        }
    }

    private void handleQueue(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final QueueConfig qConfig = new QueueConfig();
        qConfig.setName(name);
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String nodeName = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            
            if ("max-size-per-jvm".equals(nodeName)) {
                qConfig.setMaxSizePerJVM(getIntegerValue("max-size-per-jvm", value, QueueConfig.DEFAULT_MAX_SIZE_PER_JVM));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                qConfig.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value, QueueConfig.DEFAULT_TTL_SECONDS));
            }
        }
        mapQueueConfigs.put(name, qConfig);
    }

    private void handleMap(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final MapConfig config = new MapConfig();
        config.setName(name);
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String nodeName = n.getNodeName().toLowerCase();
            final String value = getTextContent(n).trim();
            
            if ("backup-count".equals(nodeName)) {
                config.setBackupCount(getIntegerValue("backup-count", value, MapConfig.DEFAULT_BACKUP_COUNT));
            } else if ("eviction-policy".equals(nodeName)) {
                config.setEvictionPolicy(value);
            } else if ("max-size".equals(nodeName)) {
                config.setMaxSize(getIntegerValue("max-size", value,
                        MapConfig.DEFAULT_MAX_SIZE));
            } else if ("eviction-percentage".equals(nodeName)) {
                config.setEvictionPercentage(getIntegerValue("eviction-percentage", value,
                        MapConfig.DEFAULT_EVICTION_PERCENTAGE));
            } else if ("time-to-live-seconds".equals(nodeName)) {
                config.setTimeToLiveSeconds(getIntegerValue("time-to-live-seconds", value,
                        MapConfig.DEFAULT_TTL_SECONDS));
            }
        }
        mapMapConfigs.put(name, config);
    }

    private void handleTcpIp(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = getTextContent(att).trim();
            if (att.getNodeName().equals("enabled")) {
                join.joinMembers.enabled = checkTrue(value);
            } else if (att.getNodeName().equals("conn-timeout-seconds")) {
                join.joinMembers.connectionTimeoutSeconds = getIntegerValue("conn-timeout-seconds",
                        value, 5);
            }
        }

        final NodeList nodelist = node.getChildNodes();
        members:
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();

            if (n.getNodeName().equalsIgnoreCase("required-member")) {
                join.joinMembers.requiredMember = value;
            } else if (n.getNodeName().equalsIgnoreCase("hostname")) {
                join.joinMembers.add(value);
            } else if (n.getNodeName().equalsIgnoreCase("interface")) {
                final int indexStar = value.indexOf('*');
                final int indexDash = value.indexOf('-');

                if (indexStar == -1 && indexDash == -1) {
                    join.joinMembers.add(value);
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
                            join.joinMembers.add(first3 + "." + String.valueOf(j));
                        }
                    } else if (lastOne.indexOf('-') != -1) {
                        final int start = Integer.parseInt(lastOne.substring(0, lastOne
                                .indexOf('-')));
                        final int end = Integer.parseInt(lastOne
                                .substring(lastOne.indexOf('-') + 1));
                        for (int j = start; j <= end; j++) {
                            join.joinMembers.add(first3 + "." + String.valueOf(j));
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
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("global-ordering-enabled")) {
                tConfig.setGlobalOrderingEnabled(checkTrue(value));
            }
        }
        mapTopicConfigs.put(name, tConfig);
    }

    private boolean hasTextContent(final Node child) {
        final boolean result = child.getNodeType() != Node.COMMENT_NODE
                && child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE;
        return result;
    }

    private boolean nameMatches(final String name, final String pattern) {
        final int index = pattern.indexOf('*');
        if (index == -1) {
            return name.equals(pattern);
        } else {
            final String firstPart = pattern.substring(0, index);
            final int indexFirstPart = name.indexOf(firstPart, 0);
            if (indexFirstPart == -1) {
                return false;
            }

            final String secondPart = pattern.substring(index + 1);
            final int indextSecondPart = name.indexOf(secondPart, index + 1);
            if (indextSecondPart == -1) {
                return false;
            }
            
            return true;
        }
    }

	/**
	 * @return the urlConfig
	 */
	public URL getUrlConfig() {
		return urlConfig;
	}

	/**
	 * @param urlConfig the urlConfig to set
	 */
	public void setUrlConfig(URL urlConfig) {
		this.urlConfig = urlConfig;
	}

	/**
	 * @return the xmlConfig
	 */
	public String getXmlConfig() {
		return xmlConfig;
	}

	/**
	 * @param xmlConfig the xmlConfig to set
	 */
	public void setXmlConfig(String xmlConfig) {
		this.xmlConfig = xmlConfig;
	}

	/**
	 * @return the groupName
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * @param groupName the groupName to set
	 */
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	/**
	 * @return the groupPassword
	 */
	public String getGroupPassword() {
		return groupPassword;
	}

	/**
	 * @param groupPassword the groupPassword to set
	 */
	public void setGroupPassword(String groupPassword) {
		this.groupPassword = groupPassword;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the portAutoIncrement
	 */
	public boolean isPortAutoIncrement() {
		return portAutoIncrement;
	}

	/**
	 * @param portAutoIncrement the portAutoIncrement to set
	 */
	public void setPortAutoIncrement(boolean portAutoIncrement) {
		this.portAutoIncrement = portAutoIncrement;
	}

	/**
	 * @return the interfaces
	 */
	public Interfaces getInterfaces() {
		return interfaces;
	}

	/**
	 * @param interfaces the interfaces to set
	 */
	public void setInterfaces(Interfaces interfaces) {
		this.interfaces = interfaces;
	}

	/**
	 * @return the join
	 */
	public Join getJoin() {
		return join;
	}

	/**
	 * @param join the join to set
	 */
	public void setJoin(Join join) {
		this.join = join;
	}

	/**
	 * @return the executorConfig
	 */
	public ExecutorConfig getExecutorConfig() {
		return executorConfig;
	}

	/**
	 * @param executorConfig the executorConfig to set
	 */
	public void setExecutorConfig(ExecutorConfig executorConfig) {
		this.executorConfig = executorConfig;
	}

	/**
	 * @return the mapTopicConfigs
	 */
	public Map<String, TopicConfig> getMapTopicConfigs() {
		return mapTopicConfigs;
	}

	/**
	 * @param mapTopicConfigs the mapTopicConfigs to set
	 */
	public void setMapTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
		this.mapTopicConfigs = mapTopicConfigs;
	}

	/**
	 * @return the mapQConfigs
	 */
	public Map<String, QueueConfig> getMapQConfigs() {
		return mapQueueConfigs;
	}

	/**
	 * @param mapQConfigs the mapQConfigs to set
	 */
	public void setMapQConfigs(Map<String, QueueConfig> mapQConfigs) {
		this.mapQueueConfigs = mapQConfigs;
	}

	/**
	 * @return the mapMapConfigs
	 */
	public Map<String, MapConfig> getMapMapConfigs() {
		return mapMapConfigs;
	}

	/**
	 * @param mapMapConfigs the mapMapConfigs to set
	 */
	public void setMapMapConfigs(Map<String, MapConfig> mapMapConfigs) {
		this.mapMapConfigs = mapMapConfigs;
	}

}
