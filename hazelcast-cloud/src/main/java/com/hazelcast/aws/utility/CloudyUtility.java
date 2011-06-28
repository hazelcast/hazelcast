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
import com.hazelcast.impl.Util;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.config.AbstractXmlConfigHelper.cleanNodeName;

public class CloudyUtility {
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

    public static Object unmarshalTheResponse(InputStream stream) throws IOException, JAXBException {
        Object o = parse(stream);
        if (o instanceof JAXBElement) {
            return ((JAXBElement) o).getValue();
        } else
            return o;
    }

    private static Object parse(InputStream in) {
        final DocumentBuilder builder;
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.parse(in);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Util.streamXML(doc, baos);
            final byte[] bytes = baos.toByteArray();
            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
//            System.out.println("XML is : " + Util.inputStreamToString(bais));
            Element element = doc.getDocumentElement();
            NodeHolder elementNodeHolder = new NodeHolder(element);
            List<String> names = elementNodeHolder.getSub("reservationset").getSub("item").getSub("instancesset").getList("privateipaddress");
            return names;
        } catch (ParserConfigurationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (SAXException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return new ArrayList<String>();
    }

    static class NodeHolder {
        Node node;

        public NodeHolder(Node node) {
            this.node = node;
        }

        public NodeHolder getSub(String name) {
            for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(this.node.getChildNodes())) {
                String nodeName = cleanNodeName(node.getNodeName());
                if (name.equals(nodeName)) {
                    return new NodeHolder(node);
                }
            }
            return null;
        }

        public List<String> getList(String name) {
            List<String> list = new ArrayList<String>();
            for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(this.node.getChildNodes())) {
                String nodeName = cleanNodeName(node.getNodeName());
                if ("item".equals(nodeName)) {
                    if (new NodeHolder(node).getSub("instancestate").getSub("name").getNode().getFirstChild().getNodeValue().equals("running")) {
                        String ip = new NodeHolder(node).getSub(name).getNode().getFirstChild().getNodeValue();
                        if (ip != null) {
                            list.add(ip);
                        }
                    }
                }
            }
            return list;
        }

        public Node getNode() {
            return node;
        }
    }
}
