/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.replacer;

import com.hazelcast.internal.nio.IOUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.StringUtil.trim;
import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static java.lang.String.format;

/**
 * This class is an <b>example</b> {@link com.hazelcast.config.replacer.spi.ConfigReplacer} implementation which decrypts
 * encrypted values.
 * <p>
 * The {@link #main(String...)} method is provided to generate the encrypted variables.
 * <p>
 * This class extends {@link AbstractPbeReplacer} where the main encryption logic is located. This class implements
 * {@link #getPassword()} method and depending on configuration allows to use a password file and/or user properties (name and
 * HOME) and/or network interface properties (MAC address).
 */
public class EncryptionReplacer extends AbstractPbeReplacer {

    /**
     * Replacer property name to configure {@code true}/{@code false} flag contolling if users properties should be used as part
     * of the encryption password.
     */
    public static final String PROPERTY_PASSWORD_USER_PROPERTIES = "passwordUserProperties";
    /**
     * Replacer property name to configure network interface name used to retrieve MAC address used as part of the encryption
     * password.
     */
    public static final String PROPERTY_PASSWORD_NETWORK_INTERFACE = "passwordNetworkInterface";
    /**
     * Replacer property name to configure path to a password file which content should be used as part of the encryption
     * password.
     */
    public static final String PROPERTY_PASSWORD_FILE = "passwordFile";

    private static final String PREFIX = "ENC";
    private static final int DEFAULT_ITERATIONS = 531;

    private boolean passwordUserProperties;
    private String passwordNetworkInterface;
    private String passwordFile;

    @Override
    public void init(Properties properties) {
        super.init(properties);
        passwordFile = properties.getProperty(PROPERTY_PASSWORD_FILE);
        passwordUserProperties = Boolean.parseBoolean(properties.getProperty(PROPERTY_PASSWORD_USER_PROPERTIES, "true"));
        passwordNetworkInterface = properties.getProperty(PROPERTY_PASSWORD_NETWORK_INTERFACE);
        checkFalse(passwordFile == null && passwordNetworkInterface == null && !passwordUserProperties,
                "At least one of the properties used to generate encryption password has to be configured");
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    protected char[] getPassword() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            if (passwordFile != null) {
                FileInputStream fis = new FileInputStream(passwordFile);
                try {
                    baos.write(IOUtil.toByteArray(fis));
                } finally {
                    IOUtil.closeResource(fis);
                }
            }
            if (passwordUserProperties) {
                baos.write(System.getProperty("user.home").getBytes(StandardCharsets.UTF_8));
                baos.write(System.getProperty("user.name").getBytes(StandardCharsets.UTF_8));
            }
            if (passwordNetworkInterface != null) {
                try {
                    NetworkInterface iface = NetworkInterface.getByName(passwordNetworkInterface);
                    baos.write(iface.getHardwareAddress());
                } catch (SocketException e) {
                    throw rethrow(e);
                }
            }
            return new String(Base64.getEncoder().encode(baos.toByteArray()), StandardCharsets.UTF_8).toCharArray();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public static final void main(String... args) throws Exception {
        if (args == null || args.length < 1 || args.length > 2) {
            System.err.println("Usage:");
            System.err.println("\tjava -D<propertyName>=<propertyValue>  " + EncryptionReplacer.class.getName()
                    + " \"<String To Encrypt>\" [iterations]");
            System.err.println();
            System.err.println("The replacer configuration can be loaded either from hazelcast/hazelcast-client XML file:");
            System.err.println("\t-Dhazelcast.config=/path/to/hazelcast.xml");
            System.err.println();
            System.err.println("or provided directly via following property names:");
            System.err.println("\t" + PROPERTY_CIPHER_ALGORITHM);
            System.err.println("\t" + PROPERTY_KEY_LENGTH_BITS);
            System.err.println("\t" + PROPERTY_SALT_LENGTH_BYTES);
            System.err.println("\t" + PROPERTY_SECRET_KEY_ALGORITHM);
            System.err.println("\t" + PROPERTY_SECRET_KEY_FACTORY_ALGORITHM);
            System.err.println("\t" + PROPERTY_SECURITY_PROVIDER);
            System.err.println("\t" + PROPERTY_PASSWORD_FILE);
            System.err.println("\t" + PROPERTY_PASSWORD_NETWORK_INTERFACE);
            System.err.println("\t" + PROPERTY_PASSWORD_USER_PROPERTIES);
            System.err.println();
            System.err.println("Values available for property " + PROPERTY_PASSWORD_NETWORK_INTERFACE);
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                byte[] hardwareAddress = networkInterface.getHardwareAddress();
                if (hardwareAddress != null) {
                    System.err.println("\t" + networkInterface.getName());
                }
            }
            System.err.println();
            System.exit(1);
        }
        System.out.println(encrypt(args));
    }

    protected static String encrypt(String... args) throws Exception {
        int iterations = args.length == 2 ? Integer.parseInt(args[1]) : DEFAULT_ITERATIONS;
        EncryptionReplacer replacer = new EncryptionReplacer();
        String xmlPath = System.getProperty("hazelcast.config");
        Properties properties = xmlPath == null ? System.getProperties()
                : loadPropertiesFromConfig(new FileInputStream(xmlPath));
        replacer.init(properties);
        String encrypted = replacer.encrypt(args[0], iterations);
        String variable = "$" + replacer.getPrefix() + "{" + encrypted + "}";
        return variable;
    }

    private static Properties loadPropertiesFromConfig(FileInputStream fileInputStream) throws Exception {
        try {
            DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
            Document doc = builder.parse(fileInputStream);
            Element root = doc.getDocumentElement();
            return loadProperties(findReplacerDefinition(root));
        } finally {
            closeResource(fileInputStream);
        }
    }

    private static Node findReplacerDefinition(Element root) throws XPathException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        xpath.setNamespaceContext(new HzNsContext());
        String xpathExp = "//%s:config-replacers/%1$s:replacer[@class-name='%s']";
        NodeList replaceTags = (NodeList) xpath.evaluate(format(xpathExp, "hz", EncryptionReplacer.class.getName()), root,
                XPathConstants.NODESET);
        if (replaceTags.getLength() < 1) {
            replaceTags = (NodeList) xpath.evaluate(format(xpathExp, "hz-client", EncryptionReplacer.class.getName()), root,
                    XPathConstants.NODESET);
            checkPositive(replaceTags.getLength(), "No EncryptionReplacer definition found within the provided XML document.");
        }

        return replaceTags.item(0);
    }

    private static Properties loadProperties(Node node) {
        Properties properties = new Properties();
        for (Node n : childElements(node)) {
            String value = cleanNodeName(n);
            if ("properties".equals(value)) {
                fillProperties(n, properties);
            }
        }
        return properties;
    }

    private static void fillProperties(Node node, Properties properties) {
        if (properties == null) {
            return;
        }
        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if ("property".equals(name)) {
                String propertyName = getTextContent(n.getAttributes().getNamedItem("name"));
                String value = trim(getTextContent(n));
                properties.setProperty(propertyName, value == null ? "" : value);
            }
        }
    }

    private static String getTextContent(Node node) {
        try {
            return node.getTextContent();
        } catch (Exception e) {
            return getTextContentOld(node);
        }
    }

    private static String getTextContentOld(Node node) {
        Node child = node.getFirstChild();
        if (child != null) {
            Node next = child.getNextSibling();
            if (next == null) {
                return hasTextContent(child) ? child.getNodeValue() : null;
            }
            StringBuilder buf = new StringBuilder();
            appendTextContents(node, buf);
            return buf.toString();
        }
        return null;
    }

    private static void appendTextContents(Node node, StringBuilder buf) {
        Node child = node.getFirstChild();
        while (child != null) {
            if (hasTextContent(child)) {
                buf.append(child.getNodeValue());
            }
            child = child.getNextSibling();
        }
    }

    private static boolean hasTextContent(Node node) {
        short nodeType = node.getNodeType();
        return nodeType != Node.COMMENT_NODE && nodeType != Node.PROCESSING_INSTRUCTION_NODE;
    }

    private static class HzNsContext implements NamespaceContext {

        @Override
        public String getNamespaceURI(String prefix) {
            if ("hz".equals(prefix)) {
                return "http://www.hazelcast.com/schema/config";
            } else if ("hz-client".equals(prefix)) {
                return "http://www.hazelcast.com/schema/client-config";
            }
            return null;
        }

        @Override
        public String getPrefix(String namespaceURI) {
            return null;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Iterator getPrefixes(String namespaceURI) {
            return null;
        }

    }
}
