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

package com.hazelcast.config;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.TypeInfo;
import org.w3c.dom.traversal.DocumentTraversal;
import org.w3c.dom.traversal.NodeFilter;
import org.w3c.dom.traversal.NodeIterator;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.w3c.dom.TypeInfo.DERIVATION_RESTRICTION;

/**
 * Test cases specific only to XML based configuration. The cases not
 * XML specific should be added to {@link XMLConfigBuilderTest}.
 * <p>
 * This test class is expected to contain only <strong>extra</strong> test
 * cases over the ones defined in {@link XMLConfigBuilderTest} in order
 * to cover XML specific cases where XML configuration derives from the
 * YAML configuration to allow usage of XML-native constructs.
 *
 * @see XMLConfigBuilderTest
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlOnlyConfigBuilderTest {

    private static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    private static final String HAZELCAST_END_TAG = "</hazelcast>\n";

    @Test(expected = InvalidConfigurationException.class)
    public void testMissingNamespace() {
        String xml = "<hazelcast/>";
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidNamespace() {
        String xml = "<hazelcast xmlns=\"http://foo.bar\"/>";
        buildConfig(xml);
    }

    @Test
    public void testValidNamespace() {
        String xml = HAZELCAST_START_TAG + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastTagAppearsTwice() {
        String xml = HAZELCAST_START_TAG + "<hazelcast/>" + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testHazelcastInstanceNameEmpty() {
        String xml = HAZELCAST_START_TAG + "<instance-name></instance-name>" + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test
    public void testXsdVersion() {
        String origVersionOverride = System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        assertXsdVersion("0.0", "0.0");
        assertXsdVersion("3.9", "3.9");
        assertXsdVersion("3.9-SNAPSHOT", "3.9");
        assertXsdVersion("3.9.1-SNAPSHOT", "3.9");
        assertXsdVersion("3.10", "3.10");
        assertXsdVersion("3.10-SNAPSHOT", "3.10");
        assertXsdVersion("3.10.1-SNAPSHOT", "3.10");
        assertXsdVersion("99.99.99", "99.99");
        assertXsdVersion("99.99.99-SNAPSHOT", "99.99");
        assertXsdVersion("99.99.99-Beta", "99.99");
        assertXsdVersion("99.99.99-Beta-SNAPSHOT", "99.99");
        if (origVersionOverride != null) {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, origVersionOverride);
        } else {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    @Test
    public void testConfig2Xml2DefaultConfig() {
        testConfig2Xml2Config("hazelcast-default.xml");
    }

    @Test
    public void testConfig2Xml2FullConfig() {
        testConfig2Xml2Config("hazelcast-fullconfig.xml");
    }

    @Test
    public void testConfig2Xml2Config_withAdvancedNetworkConfig() {
        testConfig2Xml2Config("hazelcast-fullconfig-advanced-network-config.xml");
    }

    @Test
    public void testXSDDefaultXML() throws Exception {
        testXSDConfigXML("hazelcast-default.xml");
    }

    @Test
    public void testFullConfig() throws Exception {
        testXSDConfigXML("hazelcast-fullconfig.xml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute></attribute>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAttributeConfig_noName_noExtractor_singleTag() {
        String xml = HAZELCAST_START_TAG
                + "   <map name=\"people\">\n"
                + "       <attributes>\n"
                + "           <attribute/>\n"
                + "       </attributes>"
                + "   </map>"
                + HAZELCAST_END_TAG;
        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testCacheConfig_withInvalidEvictionConfig_failsFast() {
        String xml = HAZELCAST_START_TAG
                + "    <cache name=\"cache\">"
                + "        <eviction size=\"10000000\" max-size-policy=\"ENTRY_COUNT\" eviction-policy=\"INVALID\"/>"
                + "    </cache>"
                + HAZELCAST_END_TAG;

        buildConfig(xml);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testInvalidRootElement() {
        String xml = "<hazelcast-client>"
                + "<cluster-name>dev</cluster-name>"
                + "</hazelcast-client>";
        buildConfig(xml);
    }

    @Test
    public void testAddWhitespaceToNonSpaceStrings() throws Exception {
        // parse the default config file
        InputStream xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResourceAsStream("hazelcast-default.xml");
        DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
        Document doc = builder.parse(xmlResource);
        // validate to augment with type information
        Validator validator = getValidator();
        DOMResult result = new DOMResult();
        validator.validate(new DOMSource(doc), result);
        Document validated = (Document) result.getNode();
        // add whitespace to non-space-string nodes
        assertTrue("No whitespace added", addWhitespaceToNonSpaceStrings(validated));
        String xml = serialize(validated);
        buildConfig(xml);
    }

    private static void assertXsdVersion(String buildVersion, String expectedXsdVersion) {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, buildVersion);
        assertEquals("Unexpected release version retrieved for build version " + buildVersion, expectedXsdVersion,
                new XmlConfigBuilder().getReleaseVersion());
    }

    private static void testConfig2Xml2Config(String fileName) {
        Config config = new ClasspathXmlConfig(fileName);

        String xml = new ConfigXmlGenerator(true, false).generate(config);
        Config config2 = new InMemoryXmlConfig(xml);

        assertTrue(ConfigCompatibilityChecker.isCompatible(config, config2));
    }

    private static Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private static void testXSDConfigXML(String xmlFileName) throws Exception {
        InputStream xmlResource = XMLConfigBuilderTest.class.getClassLoader().getResourceAsStream(xmlFileName);
        Source source = new StreamSource(xmlResource);
        Validator validator = getValidator();

        try {
            validator.validate(source);
        } catch (SAXException ex) {
            fail(xmlFileName + " is not valid because: " + ex.toString());
        }
    }

    private static Validator getValidator() throws SAXException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL schemaResource = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-config-"
                + Versions.CURRENT_CLUSTER_VERSION + ".xsd");
        Schema schema = factory.newSchema(schemaResource);
        return schema.newValidator();
    }

    private static String serialize(Node node) throws TransformerException {
        StringWriter result = new StringWriter();
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(new DOMSource(node), new StreamResult(result));
        return result.toString();
    }

    private static boolean addWhitespaceToNonSpaceStrings(Document doc) {
        NodeIterator nodeIterator = ((DocumentTraversal) doc).createNodeIterator(
                doc.getDocumentElement(), NodeFilter.SHOW_ELEMENT, null, true);
        boolean added = false;
        Node node;
        while ((node = nodeIterator.nextNode()) != null) {
            if (isNonSpaceString(node)) {
                addWhitespace(node);
                added = true;
            }
            NamedNodeMap attrs = node.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Node attr = attrs.item(i);
                if (isNonSpaceString(attr)) {
                    addWhitespace(attr);
                    added = true;
                }
            }
        }
        return added;
    }

    private static void addWhitespace(Node node) {
        node.setNodeValue(" \n " + node.getNodeValue() + " \n ");
    }

    private static boolean isNonSpaceString(Node node) {
        TypeInfo typeInfo;
        if (node.getNodeType() == Node.ELEMENT_NODE) {
            typeInfo = ((Element) node).getSchemaTypeInfo();
        } else if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
            typeInfo = ((Attr) node).getSchemaTypeInfo();
        } else {
            typeInfo = null;
        }
        return typeInfo != null && typeInfo.isDerivedFrom("http://www.hazelcast.com/schema/config",
                "non-space-string", DERIVATION_RESTRICTION);
    }
}
