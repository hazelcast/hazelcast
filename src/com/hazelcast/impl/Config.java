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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.hazelcast.impl.ClusterManager.JoinRequest;

public class Config {

	private static Config instance = new Config();

	boolean domLevel3 = true;
	URL urlConfig = null;
	String xmlConfig = null;

	private Config() {
		boolean usingSystemConfig = false;
		String configFile = System.getProperty("hazelcast.config");
		try {
			InputStream in = null;
			if (configFile != null) {
				File fileConfig = new File(configFile);
				if (!fileConfig.exists()) {
					String msg = "Config file at '" + configFile + "' doesn't exist.";
					msg += "\nHazelcast will try to use the hazelcast.xml config file in the classpath.";
					Util.logWarning(msg);
				} else {
					try {
						in = new FileInputStream(fileConfig);
						urlConfig = fileConfig.toURL();
						usingSystemConfig = true;
					} catch (Exception e) {
						String msg = "Having problem reading config file at '" + configFile + "'.";
						msg += "\nException message: " + e.getMessage();
						msg += "\nHazelcast will try to use the hazelcast.xml config file in the jar.";
						Util.logWarning(msg);
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
					Util.logWarning(msg);
				}
			}
			if (in == null)
				return;

			DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = null;
			try {
				doc = builder.parse(in);
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Util.streamXML(doc, baos);
				byte[] bytes = baos.toByteArray();
				ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				xmlConfig = Util.inputStreamToString(bais);
				if ("true".equals(System.getProperty("hazelcast.config.print"))) {
					System.out.println("Hazelcast config URL : " + urlConfig);
					System.out.println("=== Hazelcast config xml ===");
					System.out.println(xmlConfig);
					System.out.println("==============================");
					System.out.println("");
				}
			} catch (Exception e) {
				String msgPart = "config file '" + configFile + "' set as a system property.";
				if (!usingSystemConfig)
					msgPart = "hazelcast.xml config file in the classpath.";
				String msg = "Having problem parsing the " + msgPart;
				msg += "\nException: " + e.getMessage();
				msg += "\nHazelcast will start with default configuration.";
				Util.logWarning(msg);
				return;
			}
			Element docElement = doc.getDocumentElement();
			try {
				docElement.getTextContent();
			} catch (Throwable e) {
				domLevel3 = false;
			}
			NodeList nodelist = docElement.getChildNodes();
			for (int i = 0; i < nodelist.getLength(); i++) {
				org.w3c.dom.Node node = nodelist.item(i);
				if (node.getNodeName().equals("network")) {
					handleNetwork(node);
				} else if (node.getNodeName().equals("group")) {
					handleGroup(node);
				} else if (node.getNodeName().equals("executor-service")) {
					handleExecutor(node);
				} else if (node.getNodeName().equals("queue")) {
					handleQueue(node);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void handleNetwork(org.w3c.dom.Node node) {
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node child = nodelist.item(i);
			if (child.getNodeName().equals("port")) {
				handlePort(child);
			} else if (child.getNodeName().equals("join")) {
				handleJoin(child);
			} else if (child.getNodeName().equals("interfaces")) {
				handleInterfaces(child);
			}
		}

	}

	public QConfig getQConfig(String name) {
		Set<String> qNames = mapQConfigs.keySet();
		for (String pattern : qNames) {
			if (nameMatches(name, pattern)) {
				return mapQConfigs.get(pattern);
			}
		}
		return mapQConfigs.get("default");
	}

	private boolean nameMatches(String name, String pattern) {
		int index = pattern.indexOf('*');
		if (index == -1) {
			return name.equals(pattern);
		} else {
			String firstPart = pattern.substring(0, index);
			int indexFirstPart = name.indexOf(firstPart, 0);
			if (indexFirstPart == -1)
				return false;

			String secondPart = pattern.substring(index + 1);
			int indextSecondPart = name.indexOf(secondPart, index + 1);
			if (indextSecondPart == -1)
				return false;
			return true;
		}
	}

	private void handleJoin(org.w3c.dom.Node node) {
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node child = nodelist.item(i);
			if (child.getNodeName().equalsIgnoreCase("multicast")) {
				handleMulticast(child);
			} else if (child.getNodeName().equalsIgnoreCase("tcp-ip")) {
				handleTcpIp(child);
			}
		}
	}

	private String getTextContent(Node node) {
		if (domLevel3)
			return node.getTextContent();
		else
			return getTextContent2(node);
	}

	private String getTextContent2(Node node) {
		Node child = node.getFirstChild();
		if (child != null) {
			Node next = child.getNextSibling();
			if (next == null) {
				return hasTextContent(child) ? child.getNodeValue() : "";
			}
			StringBuffer buf = new StringBuffer();
			getTextContent2(node, buf);
			return buf.toString();
		}
		return "";
	}

	private void getTextContent2(Node node, StringBuffer buf) {
		Node child = node.getFirstChild();
		while (child != null) {
			if (hasTextContent(child)) {
				getTextContent2(child, buf);
			}
			child = child.getNextSibling();
		}

	}

	private boolean hasTextContent(Node child) {
		boolean result = child.getNodeType() != Node.COMMENT_NODE
				&& child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE;
		return result;
	}

	private int getIntegerValue(String parameterName, String value, int defaultValue) {
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			System.out.println(parameterName + " parameter value, [" + value
					+ "], is not a proper integer. Default value, [" + defaultValue
					+ "], will be used!");
			e.printStackTrace();
			return defaultValue;
		}
	}

	private void handleTcpIp(org.w3c.dom.Node node) {
		NamedNodeMap atts = node.getAttributes();
		for (int a = 0; a < atts.getLength(); a++) {
			org.w3c.dom.Node att = atts.item(a);
			String value = getTextContent(att).trim();
			if (att.getNodeName().equals("enabled")) {
				join.joinMembers.enabled = checkTrue(value);
			} else if (att.getNodeName().equals("conn-timeout-seconds")) {
				join.joinMembers.connectionTimeoutSeconds = getIntegerValue("conn-timeout-seconds",
						value, 5);
			}
		}

		NodeList nodelist = node.getChildNodes();
		members: for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			
			if (n.getNodeName().equalsIgnoreCase("required-member")) {
				join.joinMembers.requiredMember = value;
			} else if (n.getNodeName().equalsIgnoreCase("hostname")) {
				join.joinMembers.add(value);
			} else if (n.getNodeName().equalsIgnoreCase("interface")) {
				int indexStar = value.indexOf('*');
				int indexDash = value.indexOf('-');

				if (indexStar == -1 && indexDash == -1)
					join.joinMembers.add(value);
				else {
					String first3 = value.substring(0, value.lastIndexOf('.'));
					String lastOne = value.substring(value.lastIndexOf('.') + 1);
					if (first3.indexOf('*') != -1 && first3.indexOf('-') != -1) {
						String msg = "First 3 parts of interface definition cannot contain '*' and '-'.";
						msg += "\nPlease change the value '" + value + "' in the config file.";
						Util.logWarning(msg);
						continue members;
					}
					if (lastOne.equals("*")) {
						for (int j = 0; j < 256; j++) {
							join.joinMembers.add(first3 + "." + String.valueOf(j));
						}
					} else if (lastOne.indexOf('-') != -1) {
						int start = Integer.parseInt(lastOne.substring(0, lastOne.indexOf('-')));
						int end = Integer.parseInt(lastOne.substring(lastOne.indexOf('-') + 1));
						for (int j = start; j <= end; j++) {
							join.joinMembers.add(first3 + "." + String.valueOf(j));
						}
					}
				}

			}
		}
	}

	private void handlePort(org.w3c.dom.Node node) {
		String portStr = getTextContent(node).trim();
		if (portStr != null && portStr.length() > 0) {
			port = Integer.parseInt(portStr);
		}
		NamedNodeMap atts = node.getAttributes();
		for (int a = 0; a < atts.getLength(); a++) {
			org.w3c.dom.Node att = atts.item(a);
			String value = getTextContent(att).trim();
			if (att.getNodeName().equals("auto-increment")) {
				portAutoIncrement = checkTrue(value);
			}
		}
	}

	private void handleGroup(org.w3c.dom.Node node) {
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			if (n.getNodeName().equalsIgnoreCase("name")) {
				groupName = value;
			} else if (n.getNodeName().equalsIgnoreCase("password")) {
				groupPassword = value;
			}
		}
	}

	private void handleQueue(org.w3c.dom.Node node) {
		Node attName = node.getAttributes().getNamedItem("name");
		String name = getTextContent(attName);
		QConfig qConfig = new QConfig();
		qConfig.name = name;
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			if (n.getNodeName().equalsIgnoreCase("max-size-per-jvm")) {
				qConfig.maxSizePerJVM = getIntegerValue("max-size-per-jvm", value,
						Integer.MAX_VALUE);
			}
		}
		mapQConfigs.put(name, qConfig);
	}

	private void handleExecutor(org.w3c.dom.Node node) {
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			if (n.getNodeName().equalsIgnoreCase("core-pool-size")) {
				executorConfig.corePoolSize = getIntegerValue("core-pool-size", value, 10);
			} else if (n.getNodeName().equalsIgnoreCase("max-pool-size")) {
				executorConfig.maxPoolsize = getIntegerValue("max-pool-size", value, 50);
			} else if (n.getNodeName().equalsIgnoreCase("keep-alive-seconds")) {
				executorConfig.keepAliveSeconds = getIntegerValue("keep-alive-seconds", value, 50);
			}
		}
	}

	private void handleMulticast(org.w3c.dom.Node node) {
		NamedNodeMap atts = node.getAttributes();
		for (int a = 0; a < atts.getLength(); a++) {
			org.w3c.dom.Node att = atts.item(a);
			String value = getTextContent(att).trim();
			if (att.getNodeName().equals("enabled")) {
				join.multicastConfig.enabled = checkTrue(value);
			}
		}
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			if (n.getNodeName().equalsIgnoreCase("multicast-group")) {
				join.multicastConfig.multicastGroup = value;
			} else if (n.getNodeName().equalsIgnoreCase("multicast-port")) {
				join.multicastConfig.multicastPort = Integer.parseInt(value);
			}
		}
	}

	private void handleInterfaces(org.w3c.dom.Node node) {
		NamedNodeMap atts = node.getAttributes();
		for (int a = 0; a < atts.getLength(); a++) {
			org.w3c.dom.Node att = atts.item(a);
			String value = att.getNodeValue();
			if (att.getNodeName().equals("enabled")) {
				interfaces.enabled = checkTrue(value);
			}
		}
		NodeList nodelist = node.getChildNodes();
		for (int i = 0; i < nodelist.getLength(); i++) {
			org.w3c.dom.Node n = nodelist.item(i);
			String value = getTextContent(n).trim();
			if (n.getNodeName().equalsIgnoreCase("interface")) {
				interfaces.lsInterfaces.add(value);
			}
		}
	}

	public boolean checkTrue(String value) {
		if (value.equalsIgnoreCase("true"))
			return true;
		else if (value.equalsIgnoreCase("yes"))
			return true;
		else if (value.equalsIgnoreCase("on"))
			return true;
		return false;
	}

	public static Config get() {
		return instance;
	}

	public String groupName = "group-dev";

	public String groupPassword = "group-pass";

	public int groupMembershipType = JoinRequest.MEMBER;

	public int port = 5701;

	public boolean portAutoIncrement = true;

	public Interfaces interfaces = new Interfaces();

	public Join join = new Join();

	public ExecutorConfig executorConfig = new ExecutorConfig();

	public Map<String, QConfig> mapQConfigs = new HashMap<String, QConfig>();

	public class QConfig {
		public String name;

		public int maxSizePerJVM = Integer.MAX_VALUE;
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

		public void add(String member){
				lsMembers.add(member);
		}
	}

	public class MulticastConfig {

		public boolean enabled = true;

		public String multicastGroup = "224.2.2.3";

		public int multicastPort = 54327;
	}

	public class ExecutorConfig {
		public int corePoolSize = 10;

		public int maxPoolsize = 50;

		public long keepAliveSeconds = 60;
	}

	public class Interfaces {
		public boolean enabled = false;

		public List<String> lsInterfaces = new ArrayList<String>();
	}
}
