package com.hazelcast.config;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

public class XMLConfigBuilderTest {

    @Test
    public void testCleanNodeName() {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        assertEquals("nocolon", configBuilder.cleanNodeName("noColon"));
        assertEquals("after", configBuilder.cleanNodeName("Before:After"));
        assertNull(configBuilder.cleanNodeName((String) null));
    }

    @Test
    public void readAwsConfig() {
        String xml =
                "<hazelcast>\n" +
                        "   <group>\n" +
                        "        <name>dev</name>\n" +
                        "        <password>dev-pass</password>\n" +
                        "    </group>\n" +
                        "    <network>\n" +
                        "        <port auto-increment=\"true\">5701</port>\n" +
                        "        <join>\n" +
                        "            <multicast enabled=\"false\">\n" +
                        "                <multicast-group>224.2.2.3</multicast-group>\n" +
                        "                <multicast-port>54327</multicast-port>\n" +
                        "            </multicast>\n" +
                        "            <tcp-ip enabled=\"false\">\n" +
                        "                <interface>127.0.0.1</interface>\n" +
                        "            `</tcp-ip>\n" +
                        "            <aws enabled=\"true\">\n" +
                        "                <access-key>access</access-key>\n" +
                        "                <secret-key>secret</secret-key>\n" +
                        "            </aws>\n" +
                        "        </join>\n" +
                        "        <interfaces enabled=\"false\">\n" +
                        "            <interface>10.10.1.*</interface>\n" +
                        "        </interfaces>\n" +
                        "    </network>\n" +
                        "</hazelcast>";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config config = configBuilder.build();
        AwsConfig awsConfig = config.getNetworkConfig().getJoin().getAwsConfig();
        assertTrue(awsConfig.isEnabled());
        assertEquals("access", awsConfig.getAccessKey());
        assertEquals("secret", awsConfig.getSecretKey());
    }

    @Test
    @Ignore
    public void testXSDDefaultXML() throws SAXException, IOException {
        SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        URL schemaUrl = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-basic.xsd");
        URL xmlURL = XMLConfigBuilderTest.class.getClassLoader().getResource("hazelcast-default.xml");
        File schemaFile = new File(schemaUrl.getFile());
        File defaultXML = new File(xmlURL.getFile());
        Schema schema = factory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        Source source = new StreamSource(defaultXML);
        try {
            validator.validate(source);
            System.out.println(defaultXML + " is valid.");
        } catch (SAXException ex) {
            fail(defaultXML + " is not valid because: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
