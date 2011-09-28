/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.aws.utility;

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.AwsConfig;
import com.hazelcast.impl.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static com.hazelcast.config.AbstractXmlConfigHelper.cleanNodeName;

public class CloudyUtility {
    final static ILogger logger = Logger.getLogger(CloudyUtility.class.getName());

    public static String getQueryString(Map<String, String> attributes) {
        StringBuilder query = new StringBuilder();
        for (Iterator<String> iterator = attributes.keySet().iterator(); iterator.hasNext();) {
            String key = iterator.next();
            String value = attributes.get(key);
            query.append(AwsURLEncoder.urlEncode(key)).append("=").append(AwsURLEncoder.urlEncode(value)).append("&");
        }
        String result = query.toString();
        if (result != null && !result.equals(""))
            result = "?" + result.substring(0, result.length() - 1);
        return result;
    }

    public static Object unmarshalTheResponse(InputStream stream, AwsConfig awsConfig) throws IOException {
        Object o = parse(stream, awsConfig);
        return o;
    }

    private static Object parse(InputStream in, AwsConfig awsConfig) {
        final DocumentBuilder builder;
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(in);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Util.streamXML(doc, baos);
            final byte[] bytes = baos.toByteArray();
//            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
//            Reader reader = new BufferedReader(new InputStreamReader(bais));
//            int n;
//            char[] buffer = new char[1024];
//            Writer writer = new StringWriter();
//            while ((n = reader.read(buffer)) != -1) {
//                writer.write(buffer, 0, n);
//            }
//            System.out.println(writer.toString());
            Element element = doc.getDocumentElement();
            NodeHolder elementNodeHolder = new NodeHolder(element);
            List<String> names = new ArrayList<String>();
            List<NodeHolder> reservationset = elementNodeHolder.getSubNodes("reservationset");
            for (NodeHolder reservation : reservationset) {
                List<NodeHolder> items = reservation.getSubNodes("item");
                for (NodeHolder item : items) {
                    NodeHolder instancesset = item.getSub("instancesset");
                    names.addAll(instancesset.getList("privateipaddress", awsConfig));
                }
            }
            return names;
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return new ArrayList<String>();
    }

    static class NodeHolder {
        Node node;

        public NodeHolder(Node node) {
            this.node = node;
        }

        public NodeHolder getSub(String name) {
            if (node != null) {
                for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(this.node.getChildNodes())) {
                    String nodeName = cleanNodeName(node.getNodeName());
                    if (name.equals(nodeName)) {
                        return new NodeHolder(node);
                    }
                }
            }
            return new NodeHolder(null);
        }

        public List<NodeHolder> getSubNodes(String name) {
            List<NodeHolder> list = new ArrayList<NodeHolder>();
            if (node != null) {
                for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(this.node.getChildNodes())) {
                    String nodeName = cleanNodeName(node.getNodeName());
                    if (name.equals(nodeName)) {
                        list.add(new NodeHolder(node));
                    }
                }
            }
            return list;
        }

        public List<String> getList(String name, AwsConfig awsConfig) {
            List<String> list = new ArrayList<String>();
            if (node != null) {
                for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(this.node.getChildNodes())) {
                    String nodeName = cleanNodeName(node.getNodeName());
                    if ("item".equals(nodeName)) {
                        if (new NodeHolder(node).getSub("instancestate").getSub("name").getNode().getFirstChild().getNodeValue().equals("running")) {
                            String ip = new NodeHolder(node).getSub(name).getNode().getFirstChild().getNodeValue();
                            boolean passed = applyFilter(awsConfig, node);
                            if (ip != null && passed) {
                                list.add(ip);
                            }
                        }
                    }
                }
            }
            return list;
        }

        private boolean applyFilter(AwsConfig awsConfig, Node node) {
            boolean inGroup = applyFilter(node, awsConfig.getSecurityGroupName(), "groupset", "groupname");
            return inGroup && applyTagFilter(node,awsConfig.getTagKey(), awsConfig.getTagValue());
        }

        private boolean applyFilter(Node node, String filter, String set, String filterField) {
            boolean passed = (nullOrEmpty(filter));
            if (!passed) {
                for (NodeHolder group : new NodeHolder(node).getSub(set).getSubNodes("item")) {
                    NodeHolder nh = group.getSub(filterField);
                    if (nh != null && nh.getNode().getFirstChild() != null && filter.equals(nh.getNode().getFirstChild().getNodeValue())) {
                        passed = true;
                    }
                }
            }
            return passed;
        }

        private boolean applyTagFilter(Node node, String keyExpected, String valueExpected) {
            if (nullOrEmpty(keyExpected)) {
                return true;
            } else {
                for (NodeHolder group : new NodeHolder(node).getSub("tagset").getSubNodes("item")) {
                    if (keyEquals(keyExpected, group) &&
                            (nullOrEmpty(valueExpected) || valueEquals(valueExpected, group))) {
                        return true;
                    }
                }
                return false;
            }
        }

        private boolean valueEquals(String valueExpected, NodeHolder group) {
            NodeHolder nhValue = group.getSub("value");
            return nhValue != null && nhValue.getNode().getFirstChild() != null && valueExpected.equals(nhValue.getNode().getFirstChild().getNodeValue());
        }

        private boolean nullOrEmpty(String keyExpected) {
            return keyExpected == null || keyExpected.equals("");
        }

        private boolean keyEquals(String keyExpected, NodeHolder group) {
            NodeHolder nhKey = group.getSub("key");
            return nhKey != null && nhKey.getNode().getFirstChild() != null && keyExpected.equals(nhKey.getNode().getFirstChild().getNodeValue());
        }

        public Node getNode() {
            return node;
        }
    }
}
