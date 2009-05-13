/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import org.w3c.dom.*;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Config {

    public class ExecutorConfig {
        public int corePoolSize = 10;

        public int maxPoolsize = 50;

        public long keepAliveSeconds = 60;
    }

    public class Interfaces {
        public boolean enabled = false;

        public List<String> lsInterfaces = new ArrayList<String>();
    }

    public class Join {
        public MulticastConfig multicastConfig = new MulticastConfig();

        public JoinMembers joinMembers = new JoinMembers();

    }

    public class JoinMembers {
        public int connectionTimeoutSeconds = 5;

        public boolean enabled = false;

        public List<String> lsMembers = new ArrayList<String>();

        public String requiredMember = null;

        public void add(final String member) {
            lsMembers.add(member);
        }
    }

    public class MulticastConfig {

        public boolean enabled = true;

        public String multicastGroup = "224.2.2.3";

        public int multicastPort = 54327;
    }

    public class QConfig {
        public String name;

        public int maxSizePerJVM = Integer.MAX_VALUE;

        public int timeToLiveSeconds = Integer.MAX_VALUE;
    }

    public class MapConfig {
        public String name;

        public int backupCount = 1;
    }

    public class TopicConfig {
        public String name;

        public boolean globalOrderingEnabled = false;
    }

    protected final static Logger logger = Logger.getLogger(Config.class.getName());

    private static Config instance = new Config();

    boolean domLevel3 = true;

    URL urlConfig = null;

    String xmlConfig = null;

    public String groupName = "group-dev";

    public String groupPassword = "group-pass";

    public int port = 5701;

    public boolean portAutoIncrement = true;

    public Interfaces interfaces = new Interfaces();

    public Join join = new Join();

    public ExecutorConfig executorConfig = new ExecutorConfig();

    public Map<String, TopicConfig> mapTopicConfigs = new HashMap<String, TopicConfig>();

    public Map<String, QConfig> mapQConfigs = new HashMap<String, QConfig>();

    public Map<String, MapConfig> mapMapConfigs = new HashMap<String, MapConfig>();

    private Config() {
        boolean usingSystemConfig = false;
        final String configFile = System.getProperty("hazelcast.config");
        try {
            InputStream in = null;
            if (configFile != null) {
                final File fileConfig = new File(configFile);
                if (!fileConfig.exists()) {
                    String msg = "Config file at '" + configFile + "' doesn't exist.";
                    msg += "\nHazelcast will try to use the hazelcast.xml config file in the classpath.";
                    logger.log(Level.WARNING, msg);
                } else {
                    try {
                        in = new FileInputStream(fileConfig);
                        urlConfig = fileConfig.toURL();
                        usingSystemConfig = true;
                    } catch (final Exception e) {
                        String msg = "Having problem reading config file at '" + configFile + "'.";
                        msg += "\nException message: " + e.getMessage();
                        msg += "\nHazelcast will try to use the hazelcast.xml config file in the jar.";
                        logger.log(Level.WARNING, msg);
                        in = null;
                    }
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
            if (in == null)
                return;

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
                if (!usingSystemConfig)
                    msgPart = "hazelcast.xml config file in the classpath.";
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
                if (node.getNodeName().equals("network")) {
                    handleNetwork(node);
                } else if (node.getNodeName().equals("group")) {
                    handleGroup(node);
                } else if (node.getNodeName().equals("executor-service")) {
                    handleExecutor(node);
                } else if (node.getNodeName().equals("queue")) {
                    handleQueue(node);
                } else if (node.getNodeName().equals("map")) {
                    handleMap(node);
                } else if (node.getNodeName().equals("topic")) {
                    handleTopic(node);
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    public static Config get() {
        return instance;
    }

    public boolean checkTrue(final String value) {
        if (value.equalsIgnoreCase("true"))
            return true;
        else if (value.equalsIgnoreCase("yes"))
            return true;
        else if (value.equalsIgnoreCase("on"))
            return true;
        return false;
    }

    public QConfig getQConfig(final String name) {
        final Set<String> qNames = mapQConfigs.keySet();
        for (final String pattern : qNames) {
            if (nameMatches(name, pattern)) {
                return mapQConfigs.get(pattern);
            }
        }
        QConfig defaultConfig = mapQConfigs.get("default");
        if (defaultConfig == null) {
            defaultConfig = new QConfig();
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

    public void handleNetwork(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node child = nodelist.item(i);
            if (child.getNodeName().equals("port")) {
                handlePort(child);
            } else if (child.getNodeName().equals("join")) {
                handleJoin(child);
            } else if (child.getNodeName().equals("interfaces")) {
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
        if (domLevel3)
            return node.getTextContent();
        else
            return getTextContent2(node);
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
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("core-pool-size")) {
                executorConfig.corePoolSize = getIntegerValue("core-pool-size", value, 10);
            } else if (n.getNodeName().equalsIgnoreCase("max-pool-size")) {
                executorConfig.maxPoolsize = getIntegerValue("max-pool-size", value, 50);
            } else if (n.getNodeName().equalsIgnoreCase("keep-alive-seconds")) {
                executorConfig.keepAliveSeconds = getIntegerValue("keep-alive-seconds", value, 50);
            }
        }
    }

    private void handleGroup(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            final String nodeName = n.getNodeName();
            if ("name".equalsIgnoreCase(nodeName)) {
                groupName = value;
            } else if ("password".equalsIgnoreCase(nodeName)) {
                groupPassword = value;
            }
        }
    }

    private void handleInterfaces(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        for (int a = 0; a < atts.getLength(); a++) {
            final org.w3c.dom.Node att = atts.item(a);
            final String value = att.getNodeValue();
            if (att.getNodeName().equals("enabled")) {
                interfaces.enabled = checkTrue(value);
            }
        }
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("interface")) {
                interfaces.lsInterfaces.add(value);
            }
        }
    }

    private void handleJoin(final org.w3c.dom.Node node) {
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node child = nodelist.item(i);
            if (child.getNodeName().equalsIgnoreCase("multicast")) {
                handleMulticast(child);
            } else if (child.getNodeName().equalsIgnoreCase("tcp-ip")) {
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
                join.multicastConfig.enabled = checkTrue(value);
            }
        }
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("multicast-group")) {
                join.multicastConfig.multicastGroup = value;
            } else if (n.getNodeName().equalsIgnoreCase("multicast-port")) {
                join.multicastConfig.multicastPort = Integer.parseInt(value);
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
        final QConfig qConfig = new QConfig();
        qConfig.name = name;
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("max-size-per-jvm")) {
                qConfig.maxSizePerJVM = getIntegerValue("max-size-per-jvm", value,
                        Integer.MAX_VALUE);
            } else if (n.getNodeName().equalsIgnoreCase("time-to-live-seconds")) {
                qConfig.timeToLiveSeconds = getIntegerValue("time-to-live-seconds", value,
                        Integer.MAX_VALUE);
            }
        }
        mapQConfigs.put(name, qConfig);
    }

    private void handleMap(final org.w3c.dom.Node node) {
        final Node attName = node.getAttributes().getNamedItem("name");
        final String name = getTextContent(attName);
        final MapConfig config = new MapConfig();
        config.name = name;
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("backup-count")) {
                config.backupCount = getIntegerValue("backup-count", value,
                        1);
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

                if (indexStar == -1 && indexDash == -1)
                    join.joinMembers.add(value);
                else {
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
        tConfig.name = name;
        final NodeList nodelist = node.getChildNodes();
        for (int i = 0; i < nodelist.getLength(); i++) {
            final org.w3c.dom.Node n = nodelist.item(i);
            final String value = getTextContent(n).trim();
            if (n.getNodeName().equalsIgnoreCase("global-ordering-enabled")) {
                tConfig.globalOrderingEnabled = checkTrue(value);
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
            if (indexFirstPart == -1)
                return false;

            final String secondPart = pattern.substring(index + 1);
            final int indextSecondPart = name.indexOf(secondPart, index + 1);
            if (indextSecondPart == -1)
                return false;
            return true;
        }
    }

}
