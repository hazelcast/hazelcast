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

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.InputStream;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static com.hazelcast.test.TestCollectionUtils.setOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class XmlConfigSchemaLocationTest extends HazelcastTestSupport {

    // list of schema location URLs which we do not want to check
    private static final Set<String> WHITELIST = setOf();

    private static final String XML_SCHEMA_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";
    private static final String XML_SCHEMA_LOCATION_ATTRIBUTE = "schemaLocation";

    private CloseableHttpClient httpClient;
    private DocumentBuilderFactory documentBuilderFactory;
    private Set<String> validUrlsCache;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws ParserConfigurationException {
        httpClient = HttpClients.createDefault();
        documentBuilderFactory = getNsAwareDocumentBuilderFactory();
        validUrlsCache = new HashSet<String>();
    }

    @After
    public void tearDown() {
        IOUtil.closeResource(httpClient);
    }

    @Test
    public void testSchemaLocationsExist() throws Exception {
        assumeTls12Available();

        ResourcesScanner scanner = new ResourcesScanner();
        Reflections reflections = new Reflections(scanner);
        Set<String> resources = reflections.getResources(Pattern.compile(".*\\.xml"));
        ClassLoader classLoader = getClass().getClassLoader();
        for (String resource : resources) {
            System.out.println(resource);
            URL resourceUrl = classLoader.getResource(resource);
            String protocol = resourceUrl.getProtocol();

            // do not validate schemas from JARs (libraries). we are interested in local project files only.
            if (protocol.startsWith("jar")) {
                continue;
            }

            InputStream stream = null;
            try {
                stream = classLoader.getResourceAsStream(resource);
                validateSchemaLocationUrl(stream, resource);
            } finally {
                IOUtil.closeResource(stream);
            }
        }
    }

    private void validateSchemaLocationUrl(InputStream documentStream, String originalLocation) throws Exception {
        String schemaAttr = extractSchemaAttribute(documentStream);
        if (schemaAttr == null) {
            return;
        }
        for (String nameSpaceUrl : schemaAttr.split(" ")) {
            nameSpaceUrl = nameSpaceUrl.trim();
            if (shouldSkipValidation(nameSpaceUrl)) {
                continue;
            }
            int responseCode;
            try {
                responseCode = getResponseCode(nameSpaceUrl);
            } catch (Exception e) {
                throw new IllegalStateException("Error while validating schema location '" + nameSpaceUrl + "' from '" + originalLocation + "'", e);
            }
            assertEquals("Schema location '" + nameSpaceUrl + "' from '" + originalLocation + "' does not return HTTP 200 ", 200, responseCode);
            validUrlsCache.add(nameSpaceUrl);
        }
    }

    private String extractSchemaAttribute(InputStream documentStream) throws Exception {
        DocumentBuilder parser = documentBuilderFactory.newDocumentBuilder();
        Document document = parser.parse(documentStream);

        Element item = document.getDocumentElement();
        if (item == null) {
            return null;
        }
        Attr schemaAttr = item.getAttributeNodeNS(XML_SCHEMA_NAMESPACE, XML_SCHEMA_LOCATION_ATTRIBUTE);
        if (schemaAttr == null) {
            return null;
        }
        return schemaAttr.getValue();
    }

    private boolean shouldSkipValidation(String nameSpaceUrl) {
        if (nameSpaceUrl.isEmpty()) {
            return true;
        }
        if (!nameSpaceUrl.endsWith(".xsd")) {
            return true;
        }
        if (WHITELIST.contains(nameSpaceUrl)) {
            return true;
        }
        if (validUrlsCache.contains(nameSpaceUrl)) {
            return true;
        }
        return false;
    }

    private int getResponseCode(String url) throws Exception {
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpGet);
            return response.getStatusLine().getStatusCode();
        } finally {
            IOUtil.closeResource(response);
        }
    }

    private void assumeTls12Available() {
        try {
            SSLContext.getInstance("TLSv1.2");
        } catch (NoSuchAlgorithmException e) {
            throw new AssumptionViolatedException("TLS1.2 unavailable, cannot run " + testName.getMethodName()
                    + " The test uses HTTPS to fetch XML schemas. Current Java does not support TLS1.2."
                    + " Most web servers no longer works with TLS1.1 and older so ignoring the test.", e);
        }
    }
}
