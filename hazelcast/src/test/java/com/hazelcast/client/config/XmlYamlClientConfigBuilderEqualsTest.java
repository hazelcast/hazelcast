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

package com.hazelcast.client.config;

import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlYamlClientConfigBuilderEqualsTest {

    @Test
    public void testDefaultClientConfig() {
        ClientConfig xmlConfig = new ClientClasspathXmlConfig("hazelcast-client-default.xml");
        ClientConfig yamlConfig = new ClientClasspathYamlConfig("hazelcast-client-default.yaml");

        String xmlConfigFromXml = ClientConfigXmlGenerator.generate(xmlConfig);
        String xmlConfigFromYaml = ClientConfigXmlGenerator.generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullExampleClientConfig() throws IOException {
        String fullExampleXml = readResourceToString("hazelcast-client-full-example.xml");
        String fullExampleYaml = readResourceToString("hazelcast-client-full-example.yaml");

        // remove imports to prevent the test from failing with importing non-existing files
        fullExampleXml = fullExampleXml.replace("<import resource=\"your-client-configuration-XML-file\"/>", "");
        fullExampleYaml = fullExampleYaml
                .replace("\r", "")
                .replace("import:\n    - your-client-configuration-YAML-file", "");

        ClientConfig xmlConfig = buildConfigFromXml(fullExampleXml);
        ClientConfig yamlConfig = buildConfigFromYaml(fullExampleYaml);

        String xmlConfigFromXml = ClientConfigXmlGenerator.generate(xmlConfig, 4);
        String xmlConfigFromYaml = ClientConfigXmlGenerator.generate(yamlConfig, 4);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullClientConfig() throws IOException {
        String fullConfigXml = readResourceToString("hazelcast-client-full.xml");
        String fullConfigYaml = readResourceToString("hazelcast-client-full.yaml");

        // remove imports to prevent the test from failing with importing non-existing files
        fullConfigXml = fullConfigXml.replace("<import resource=\"your-client-configuration-XML-file\"/>", "");
        fullConfigYaml = fullConfigYaml
                .replace("\r", "")
                .replace("import:\n    - your-client-configuration-YAML-file", "");

        ClientConfig xmlConfig = buildConfigFromXml(fullConfigXml);
        ClientConfig yamlConfig = buildConfigFromYaml(fullConfigYaml);

        String xmlConfigFromXml = ClientConfigXmlGenerator.generate(xmlConfig, 4);
        String xmlConfigFromYaml = ClientConfigXmlGenerator.generate(yamlConfig, 4);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullClientFailoverConfig() throws Exception {
        DeclarativeConfigFileHelper helper = new DeclarativeConfigFileHelper();
        helper.givenXmlClientConfigFileOnClasspath("your-first-hazelcast-client-configuration.xml", "instance0");
        helper.givenXmlClientConfigFileOnClasspath("your-second-hazelcast-client-configuration.xml", "instance1");
        helper.givenYamlClientConfigFileOnClasspath("your-first-hazelcast-client-configuration.yaml", "instance0");
        helper.givenYamlClientConfigFileOnClasspath("your-second-hazelcast-client-configuration.yaml", "instance1");

        try {
            ClientFailoverConfig xmlConfig = new ClientFailoverClasspathXmlConfig("hazelcast-client-failover-full-example.xml");
            ClientFailoverConfig yamlConfig = new ClientFailoverClasspathYamlConfig(
                    "hazelcast-client-failover-full-example.yaml");

            assertEquals(xmlConfig.getTryCount(), yamlConfig.getTryCount());

            ClientConfig xmlClientConfig0 = xmlConfig.getClientConfigs().get(0);
            ClientConfig xmlClientConfig1 = xmlConfig.getClientConfigs().get(1);
            ClientConfig yamlClientConfig0 = yamlConfig.getClientConfigs().get(0);
            ClientConfig yamlClientConfig1 = yamlConfig.getClientConfigs().get(1);

            assertEquals("instance0", xmlClientConfig0.getInstanceName());
            assertEquals("instance1", xmlClientConfig1.getInstanceName());
            assertEquals("instance0", yamlClientConfig0.getInstanceName());
            assertEquals("instance1", yamlClientConfig1.getInstanceName());
        } finally {
            helper.ensureTestConfigDeleted();
        }
    }

    private String readResourceToString(String resource) throws IOException {
        InputStream xmlInputStream = getClass().getClassLoader().getResourceAsStream(resource);
        return new String(IOUtil.toByteArray(xmlInputStream));
    }

    private static ClientConfig buildConfigFromXml(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        return new XmlClientConfigBuilder(bis).build();
    }

    private static ClientConfig buildConfigFromYaml(String yaml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        return new YamlClientConfigBuilder(bis).build();
    }
}
