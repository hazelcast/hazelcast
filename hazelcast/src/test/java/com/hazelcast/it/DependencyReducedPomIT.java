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

package com.hazelcast.it;

import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class DependencyReducedPomIT {

    @Test
    public void testZeroCompileScopedDeps() throws Exception {
        DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
        Document xmlDocument = null;
        try (FileInputStream fis = new FileInputStream("dependency-reduced-pom.xml")) {
            xmlDocument = builder.parse(fis);
        }
        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(new NamespaceContext() {

            @Override
            public Iterator getPrefixes(String namespaceURI) {
                return null;
            }

            @Override
            public String getPrefix(String namespaceURI) {
                return null;
            }

            @Override
            public String getNamespaceURI(String prefix) {
                if ("m".equals(prefix)) {
                    return "http://maven.apache.org/POM/4.0.0";
                }
                return null;
            }
        });
        // check first we are making valid XPath selections
        NodeList expectedProjectsList = (NodeList) xPath.compile("//m:project").evaluate(xmlDocument, XPathConstants.NODESET);
        assertEquals(1, expectedProjectsList.getLength());

        // verify there is no compile-scoped dependency
        NodeList unexpectedDepsList = (NodeList) xPath.compile("/m:project/m:dependencies/m:dependency[m:scope/text()='compile']")
                .evaluate(xmlDocument, XPathConstants.NODESET);
        for (int i = 0; i < unexpectedDepsList.getLength(); i++) {
            // make debugging easier, fail after printing out the problematic dependencies
            System.err.println("Dependency found: " + unexpectedDepsList.item(i).getTextContent());
        }
        assertEquals(expectedDependencies(), unexpectedDepsList.getLength());
    }

    protected int expectedDependencies() {
        return 0;
    }
}
