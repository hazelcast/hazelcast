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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.hazelcast.internal.config.DomConfigHelper.asElementIterable;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IterableNodeListTest {

    private Document document;

    @Before
    public void setup() throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
        final String testXml = "<rOOt><element></element><element></element><element></element></rOOt>";
        document = builder.parse(new ByteArrayInputStream(testXml.getBytes()));
    }

    @Test
    public void testIterableNodeList() {
        int count = 0;
        for (Node node : asElementIterable(document.getFirstChild().getChildNodes())) {
            count += (node != null) ? 1 : 0;
        }
        assertEquals(3, count);
    }

    @Test
    public void testHasNext() {
        assertTrue(asElementIterable(document.getChildNodes()).iterator().hasNext());
    }

    @Test
    public void testNext() {
        assertNotNull(asElementIterable(document.getChildNodes()).iterator().next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        asElementIterable(document.getChildNodes()).iterator().remove();
    }

    @Test
    public void testCleanNodeName() {
        assertEquals("root", cleanNodeName(document.getDocumentElement()));
    }

}
