package com.hazelcast.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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

public class XmlConfigBuilder {

	private final static Logger logger = Logger.getLogger(XmlConfigBuilder.class.getName());
    private boolean domLevel3 = true;
    private Config config;
    private InputStream inputStream;
	
    public XmlConfigBuilder(InputStream inputStream) {
    	this.inputStream = inputStream;
    }
    
	public void parse(final Config config) throws Exception {
		this.config = config;
		
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

        Document doc = null;
        try {
        	doc = builder.parse(inputStream);
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
        	if (!config.isUsingSystemConfig()) {
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
        final ExecutorConfig executorConfig = config.getExecutorConfig();

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
                config.setGroupName(value);
            } else if ("password".equals(nodeName)) {
                config.setGroupPassword(value);
            }
        }
    }

    private void handleInterfaces(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Interfaces interfaces = config.getInterfaces();
        
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
        final Join join = config.getJoin();
        
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
        this.config.getMapQConfigs().put(name, qConfig);
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
        this.config.getMapMapConfigs().put(name, config);
    }

    private void handleTcpIp(final org.w3c.dom.Node node) {
        final NamedNodeMap atts = node.getAttributes();
        final Join join = config.getJoin();
        
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
        config.getMapTopicConfigs().put(name, tConfig);
    }

    private boolean hasTextContent(final Node child) {
        final boolean result = child.getNodeType() != Node.COMMENT_NODE
                && child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE;
        return result;
    }
	
}
