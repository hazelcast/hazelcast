/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws.utility;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.AbstractXmlConfigHelper.childElements;
import static com.hazelcast.config.AbstractXmlConfigHelper.cleanNodeName;
import static java.lang.String.format;

public final class CloudyUtility {
    static final ILogger LOGGER = Logger.getLogger(CloudyUtility.class);

    private CloudyUtility() {
    }

    public static Map<String, String> unmarshalTheResponse(InputStream stream, AwsConfig awsConfig) throws IOException {
        final DocumentBuilder builder;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            builder = dbf.newDocumentBuilder();
            Document doc = builder.parse(stream);
            Element element = doc.getDocumentElement();
            NodeHolder elementNodeHolder = new NodeHolder(element);
            final Map<String, String> addresses = new LinkedHashMap<String, String>();
            List<NodeHolder> reservationset = elementNodeHolder.getSubNodes("reservationset");
            for (NodeHolder reservation : reservationset) {
                List<NodeHolder> items = reservation.getSubNodes("item");
                for (NodeHolder item : items) {
                    NodeHolder instancesset = item.getFirstSubNode("instancesset");
                    addresses.putAll(instancesset.getAddresses(awsConfig));
                }
            }
            return addresses;
        } catch (Exception e) {
            LOGGER.warning(e);
        }
        return new LinkedHashMap<String, String>();    }

    private static class NodeHolder {
        private final Node node;

        NodeHolder(Node node) {
            this.node = node;
        }

        public Node getNode() {
            return node;
        }

        public NodeHolder getFirstSubNode(String name) {
            if (node == null) {
                return new NodeHolder(null);
            }
            for (Node child : childElements(node)) {
                if (name.equals(cleanNodeName(child))) {
                    return new NodeHolder(child);
                }
            }
            return new NodeHolder(null);
        }

        public List<NodeHolder> getSubNodes(String name) {
            List<NodeHolder> result = new ArrayList<NodeHolder>();
            if (node == null) {
                return result;
            }
            for (Node child : childElements(node)) {
                if (name.equals(cleanNodeName(child))) {
                    result.add(new NodeHolder(child));
                }
            }
            return result;
        }

        public Map<String, String> getAddresses(AwsConfig awsConfig) {
            final Map<String, String> privatePublicPairs = new LinkedHashMap<String, String>();
            if (node == null) {
                return privatePublicPairs;
            }

            for (NodeHolder childHolder : getSubNodes("item")) {
                final String state = getState(childHolder);
                final String privateIp = getIp("privateipaddress", childHolder);
                final String publicIp = getIp("ipaddress", childHolder);
                final String instanceName = getInstanceName(childHolder);

                if (privateIp != null) {
                    final Node child = childHolder.getNode();
                    if (!acceptState(state)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason:"
                                + " the instance is not running but %s", instanceName, privateIp, state));
                    } else if (!acceptTag(awsConfig, child)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason:"
                                + " tag-key/tag-value don't match", instanceName, privateIp));
                    } else if (!acceptGroupName(awsConfig, child)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason:"
                                + " security-group-name doesn't match", instanceName, privateIp));
                    } else {
                        privatePublicPairs.put(privateIp, publicIp);
                        LOGGER.finest(format("Accepting EC2 instance [%s][%s]", instanceName, privateIp));
                    }
                }

            }
            return privatePublicPairs;
        }

        private static boolean acceptState(String state) {
            return "running".equals(state);
        }

        private static String getState(NodeHolder nodeHolder) {
            final NodeHolder instancestate = nodeHolder.getFirstSubNode("instancestate");
            return instancestate.getFirstSubNode("name").getNode().getFirstChild().getNodeValue();
        }

        private static String getInstanceName(NodeHolder nodeHolder) {
            final NodeHolder tagSetHolder = nodeHolder.getFirstSubNode("tagset");
            if (tagSetHolder.getNode() == null) {
                return null;
            }
            for (NodeHolder itemHolder : tagSetHolder.getSubNodes("item")) {
                final Node keyNode = itemHolder.getFirstSubNode("key").getNode();
                if (keyNode == null || keyNode.getFirstChild() == null) {
                    continue;
                }
                final String nodeValue = keyNode.getFirstChild().getNodeValue();
                if (!"Name".equals(nodeValue)) {
                    continue;
                }

                final Node valueNode = itemHolder.getFirstSubNode("value").getNode();
                if (valueNode == null || valueNode.getFirstChild() == null) {
                    continue;
                }
                return valueNode.getFirstChild().getNodeValue();
            }
            return null;
        }

        private static String getIp(String name, NodeHolder nodeHolder) {
            final Node child = nodeHolder.getFirstSubNode(name).getNode();
            return child == null ? null : child.getFirstChild().getNodeValue();
        }

        private static boolean acceptTag(AwsConfig awsConfig, Node node) {
            return applyTagFilter(node, awsConfig.getTagKey(), awsConfig.getTagValue());
        }

        private static boolean acceptGroupName(AwsConfig awsConfig, Node node) {
            return applyFilter(node, awsConfig.getSecurityGroupName(), "groupset", "groupname");
        }

        private static boolean applyFilter(Node node, String filter, String set, String filterField) {
            if (nullOrEmpty(filter)) {
                return true;
            } else {
                for (NodeHolder group : new NodeHolder(node).getFirstSubNode(set).getSubNodes("item")) {
                    NodeHolder nh = group.getFirstSubNode(filterField);
                    if (nh != null && nh.getNode().getFirstChild()
                            != null && filter.equals(nh.getNode().getFirstChild().getNodeValue())) {
                        return true;
                    }
                }
                return false;
            }
        }

        private static boolean applyTagFilter(Node node, String keyExpected, String valueExpected) {
            if (nullOrEmpty(keyExpected)) {
                return true;
            } else {
                for (NodeHolder group : new NodeHolder(node).getFirstSubNode("tagset").getSubNodes("item")) {
                    if (keyEquals(keyExpected, group)
                            && (nullOrEmpty(valueExpected) || valueEquals(valueExpected, group))) {
                        return true;
                    }
                }
                return false;
            }
        }

        private static boolean valueEquals(String valueExpected, NodeHolder group) {
            NodeHolder nhValue = group.getFirstSubNode("value");
            return nhValue != null && nhValue.getNode().getFirstChild()
                    != null && valueExpected.equals(nhValue.getNode().getFirstChild().getNodeValue());
        }

        private static boolean nullOrEmpty(String keyExpected) {
            return keyExpected == null || keyExpected.isEmpty();
        }

        private static boolean keyEquals(String keyExpected, NodeHolder group) {
            NodeHolder nhKey = group.getFirstSubNode("key");
            return nhKey != null && nhKey.getNode().getFirstChild()
                    != null && keyExpected.equals(nhKey.getNode().getFirstChild().getNodeValue());
        }
    }
}
