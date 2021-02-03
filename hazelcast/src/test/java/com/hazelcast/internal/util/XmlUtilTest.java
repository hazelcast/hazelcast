/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import static com.hazelcast.internal.util.XmlUtil.SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES;
import static com.hazelcast.internal.util.XmlUtil.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.validation.SchemaFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.xml.sax.SAXException;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class })
public class XmlUtilTest {

    @Rule
    public OverridePropertyRule ignoreXxeFailureProp = OverridePropertyRule
            .clear(SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES);

    @Test
    public void testFormat() throws Exception {
        assertEquals("<a> <b>c</b></a>", format("<a><b>c</b></a>", 1).replaceAll("[\r\n]", ""));
        assertEquals("<a>   <b>c</b></a>", format("<a><b>c</b></a>", 3).replaceAll("[\r\n]", ""));
        assertEquals("<a><b>c</b></a>", format("<a><b>c</b></a>", -21));

        assertThrows(IllegalArgumentException.class, () -> format("<a><b>c</b></a>", 0));

        // check if the XXE protection is enabled
        String xxeAttack = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" + "  <!DOCTYPE test [\n"
                + "    <!ENTITY xxe SYSTEM \"file:///etc/passwd\">\n" + "  ]>" + "<a><b>&xxe;</b></a>";
        assertEquals(xxeAttack, format(xxeAttack, 1));

        // wrongly formatted XML
        assertEquals("<a><b>c</b><a>", format("<a><b>c</b><a>", 1));
    }

    @Test
    public void testGetSchemaFactory() throws Exception {
        SchemaFactory schemaFactory = XmlUtil.getSchemaFactory();
        assertNotNull(schemaFactory);
        assertThrows(SAXException.class, () -> XmlUtil.setProperty(schemaFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(SAXException.class, () -> XmlUtil.setProperty(schemaFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setProperty(schemaFactory, "test://no-such-property");
    }

    @Test
    public void testGetTransformerFactory() throws Exception {
        TransformerFactory transformerFactory = XmlUtil.getTransformerFactory();
        assertNotNull(transformerFactory);
        assertThrows(IllegalArgumentException.class, () -> XmlUtil.setAttribute(transformerFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(IllegalArgumentException.class, () -> XmlUtil.setAttribute(transformerFactory, "test://no-such-property"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setAttribute(transformerFactory, "test://no-such-property");
    }

    @Test
    public void testGetDocumentBuilderFactory() throws Exception {
        DocumentBuilderFactory dbf = XmlUtil.getNsAwareDocumentBuilderFactory();
        assertNotNull(dbf);
        assertThrows(ParserConfigurationException.class, () -> XmlUtil.setFeature(dbf, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("false");
        assertThrows(ParserConfigurationException.class, () -> XmlUtil.setFeature(dbf, "test://no-such-feature"));
        ignoreXxeFailureProp.setOrClearProperty("true");
        XmlUtil.setFeature(dbf, "test://no-such-feature");
    }
}
