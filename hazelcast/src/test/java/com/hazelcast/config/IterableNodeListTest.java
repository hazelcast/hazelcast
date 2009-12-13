/**
 * 
 */
package com.hazelcast.config;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 */
public class IterableNodeListTest {

	private Document document;
	
	@Before
	public void setupNodeList() throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        final String testXml = "<root><element></element><element></element><element></element></root>";
        document = builder.parse(new ByteArrayInputStream(testXml.getBytes()));
	}
	
	/**
	 * Test method for {@link com.hazelcast.config.XmlConfigBuilder.IterableNodeList#IterableNodeList(org.w3c.dom.NodeList)}.
	 */
	@Test
	public void testIterableNodeList() {
        NodeList nodeList = document.getFirstChild().getChildNodes();
		int count = 0;
		
		for(Node node : new XmlConfigBuilder.IterableNodeList(nodeList)) {
			count++;
		}
		
		assertEquals(3, count);
	}

	/**
	 * Test method for {@link com.hazelcast.config.XmlConfigBuilder.IterableNodeList#hasNext()}.
	 */
	@Test
	public void testHasNext() {
        NodeList nodeList = document.getChildNodes();
		assertTrue(new XmlConfigBuilder.IterableNodeList(nodeList).iterator().hasNext());
	}

	/**
	 * Test method for {@link com.hazelcast.config.XmlConfigBuilder.IterableNodeList#next()}.
	 */
	@Test
	public void testNext() {
        NodeList nodeList = document.getChildNodes();
		assertNotNull(new XmlConfigBuilder.IterableNodeList(nodeList).iterator().next());
	}

	/**
	 * Test method for {@link com.hazelcast.config.XmlConfigBuilder.IterableNodeList#remove()}.
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
        NodeList nodeList = document.getChildNodes();
		new XmlConfigBuilder.IterableNodeList(nodeList).iterator().remove();
	}

}
