/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.jet.JetConfig;
import com.hazelcast.jet.impl.Util;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNull;

public class XmlConfigTest {

    private static final String TEST_XML_1 = "hazelcast-jet-test.xml";

    @Test
    public void when_noConfigSpecified_usesDefaultConfig() {
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder(new Properties());
        JetConfig jetConfig = builder.getJetConfig();

        assertNull("resourceDirectory", jetConfig.getResourceDirectory());
        assertEquals(Runtime.getRuntime().availableProcessors(), jetConfig.getExecutionThreadCount());
    }


    @Test
    public void when_filePathSpecified_usesSpecifiedFile() throws IOException {
        File tempFile = File.createTempFile("jet", ".xml");
        try (FileOutputStream os = new FileOutputStream(tempFile)) {
            InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(TEST_XML_1);
            os.write(Util.read(resourceAsStream));
        }

        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, tempFile.getAbsolutePath());
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder(properties);
        JetConfig jetConfig = builder.getJetConfig();

        assertConfig(jetConfig);
    }

    @Test
    public void when_classpathSpecified_usesSpecifiedResource() {
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:" + TEST_XML_1);
        XmlJetConfigBuilder builder = new XmlJetConfigBuilder(properties);
        JetConfig jetConfig = builder.getJetConfig();

        assertConfig(jetConfig);
    }

    @Test
    public void when_configHasVariable_variablesAreReplaced() {
        String dir = "/var/tmp";
        int threadCount = 55;
        Properties properties = new Properties();
        properties.put(XmlJetConfigLocator.HAZELCAST_JET_CONFIG_PROPERTY, "classpath:hazelcast-jet-with-variables.xml");
        properties.put("resource.directory", dir);
        properties.put("thread.count", String.valueOf(threadCount));

        XmlJetConfigBuilder builder = new XmlJetConfigBuilder(properties);
        JetConfig jetConfig = builder.getJetConfig();

        assertConfig(jetConfig);
    }

    private void assertConfig(JetConfig jetConfig) {
        assertEquals(55, jetConfig.getExecutionThreadCount());
        assertEquals("/var/tmp", jetConfig.getResourceDirectory());

        assertEquals("value1", jetConfig.getProperties().getProperty("property1"));
        assertEquals("value2", jetConfig.getProperties().getProperty("property2"));
    }
}
