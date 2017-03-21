/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.AbstractXmlConfigHelper.childElements;
import static com.hazelcast.config.AbstractXmlConfigHelper.cleanNodeName;
import static java.lang.String.format;

public final class CloudyUtility {

    private static final String NODE_ITEM = "item";
    private static final String NODE_VALUE = "value";
    private static final String NODE_KEY = "key";

    private static final ILogger LOGGER = Logger.getLogger(CloudyUtility.class);

    private CloudyUtility() {
    }

    /**
     * Unmarshal the response from {@link com.hazelcast.aws.impl.DescribeInstances} and return the discovered node map.
     * The map contains mappings from private to public IP and all contained nodes match the filtering rules defined by
     * the {@code awsConfig}.
     * If there is an exception while unmarshaling the response, returns an empty map.
     *
     * @param stream    the response XML stream
     * @param awsConfig the AWS configuration for filtering the returned addresses
     * @return map from private to public IP or empty map in case of exceptions
     */
    public static Map<String, String> unmarshalTheResponse(InputStream stream, AwsConfig awsConfig) {
        DocumentBuilder builder;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            builder = dbf.newDocumentBuilder();
            Document doc = builder.parse(stream);
            Element element = doc.getDocumentElement();
            NodeHolder elementNodeHolder = new NodeHolder(element);
            Map<String, String> addresses = new LinkedHashMap<String, String>();
            List<NodeHolder> reservationSet = elementNodeHolder.getSubNodes("reservationset");
            for (NodeHolder reservation : reservationSet) {
                List<NodeHolder> items = reservation.getSubNodes(NODE_ITEM);
                for (NodeHolder item : items) {
                    NodeHolder instancesSet = item.getFirstSubNode("instancesset");
                    addresses.putAll(instancesSet.getAddresses(awsConfig));
                }
            }
            return addresses;
        } catch (Exception e) {
            LOGGER.warning(e);
        }
        return new LinkedHashMap<String, String>();
    }

    private static class NodeHolder {

        private final Node node;

        NodeHolder(Node node) {
            this.node = node;
        }

        Node getNode() {
            return node;
        }

        NodeHolder getFirstSubNode(String name) {
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

        List<NodeHolder> getSubNodes(String name) {
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

        /**
         * Unmarshal the response from the {@link com.hazelcast.aws.impl.DescribeInstances} service and
         * return the map from private to public IP. All returned entries must match filters defined by the {@code config}.
         * This method expects that the DOM containing the XML has been positioned at the node containing the addresses.
         *
         * @param awsConfig the AWS configuration for filtering the returned addresses
         * @return map from private to public IP
         * @see #getFirstSubNode(String)
         */
        Map<String, String> getAddresses(AwsConfig awsConfig) {
            Map<String, String> privatePublicPairs = new LinkedHashMap<String, String>();
            if (node == null) {
                return privatePublicPairs;
            }

            for (NodeHolder childHolder : getSubNodes(NODE_ITEM)) {
                String state = getState(childHolder);
                String privateIp = getIp("privateipaddress", childHolder);
                String publicIp = getIp("ipaddress", childHolder);
                String instanceName = getInstanceName(childHolder);

                if (privateIp != null) {
                    Node child = childHolder.getNode();
                    if (!acceptState(state)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason: the instance is not running but %s",
                                instanceName, privateIp, state));
                    } else if (!acceptTag(awsConfig, child)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason: tag-key/tag-value don't match",
                                instanceName, privateIp));
                    } else if (!acceptGroupName(awsConfig, child)) {
                        LOGGER.finest(format("Ignoring EC2 instance [%s][%s] reason: security-group-name doesn't match",
                                instanceName, privateIp));
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
            NodeHolder instanceState = nodeHolder.getFirstSubNode("instancestate");
            return instanceState.getFirstSubNode("name").getNode().getFirstChild().getNodeValue();
        }

        private static String getInstanceName(NodeHolder nodeHolder) {
            NodeHolder tagSetHolder = nodeHolder.getFirstSubNode("tagset");
            if (tagSetHolder.getNode() == null) {
                return null;
            }
            for (NodeHolder itemHolder : tagSetHolder.getSubNodes(NODE_ITEM)) {
                Node keyNode = itemHolder.getFirstSubNode(NODE_KEY).getNode();
                if (keyNode == null || keyNode.getFirstChild() == null) {
                    continue;
                }
                String nodeValue = keyNode.getFirstChild().getNodeValue();
                if (!"Name".equals(nodeValue)) {
                    continue;
                }

                Node valueNode = itemHolder.getFirstSubNode(NODE_VALUE).getNode();
                if (valueNode == null || valueNode.getFirstChild() == null) {
                    continue;
                }
                return valueNode.getFirstChild().getNodeValue();
            }
            return null;
        }

        private static String getIp(String name, NodeHolder nodeHolder) {
            Node child = nodeHolder.getFirstSubNode(name).getNode();
            return (child == null ? null : child.getFirstChild().getNodeValue());
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
                for (NodeHolder group : new NodeHolder(node).getFirstSubNode(set).getSubNodes(NODE_ITEM)) {
                    NodeHolder nh = group.getFirstSubNode(filterField);
                    if (nh != null && nh.getNode().getFirstChild() != null
                            && filter.equals(nh.getNode().getFirstChild().getNodeValue())) {
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
                for (NodeHolder group : new NodeHolder(node).getFirstSubNode("tagset").getSubNodes(NODE_ITEM)) {
                    if (keyEquals(keyExpected, group) && (nullOrEmpty(valueExpected) || valueEquals(valueExpected, group))) {
                        return true;
                    }
                }
                return false;
            }
        }

        private static boolean valueEquals(String valueExpected, NodeHolder group) {
            NodeHolder nhValue = group.getFirstSubNode(NODE_VALUE);
            return (nhValue != null && nhValue.getNode().getFirstChild() != null
                    && valueExpected.equals(nhValue.getNode().getFirstChild().getNodeValue()));
        }

        private static boolean nullOrEmpty(String keyExpected) {
            return keyExpected == null || keyExpected.isEmpty();
        }

        private static boolean keyEquals(String keyExpected, NodeHolder group) {
            NodeHolder nhKey = group.getFirstSubNode(NODE_KEY);
            return (nhKey != null && nhKey.getNode().getFirstChild() != null
                    && keyExpected.equals(nhKey.getNode().getFirstChild().getNodeValue()));
        }
    }
}
