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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlYamlConfigBuilderEqualsTest extends HazelcastTestSupport {

    @Test
    public void testFullConfigNormalNetwork() {
        assertXmlYamlFileEquals("hazelcast-fullconfig");
    }

    @Test
    public void testFullConfigAdvancedNetwork() {
        assertXmlYamlFileEquals("hazelcast-fullconfig-advanced-network-config");
    }

    @Test
    public void testDefaultConfig() {
        assertXmlYamlFileEquals("hazelcast-default");
    }

    @Test
    public void testDefaultAssemblyConfig() {
        assertXmlYamlFileEquals("hazelcast-default-assembly");
    }

    @Test
    public void testDockerAssemblyConfig() {
        assertXmlYamlFileEquals("hazelcast-docker-assembly");
    }

    @Test
    public void testSecurityHardenedExample() {
        assertXmlYamlFileEquals("hazelcast-security-hardened");
    }

    @Test
    public void testFullExample() throws IOException {
        String fullExampleXml = readResourceToString("hazelcast-full-example.xml");
        String fullExampleYaml = readResourceToString("hazelcast-full-example.yaml");

        // remove imports to prevent the test from failing with importing non-existing files
        fullExampleXml = fullExampleXml.replace("<import resource=\"your-configuration-XML-file\"/>", "");
        fullExampleYaml = fullExampleYaml
                .replace("\r", "")
                .replace("import:\n    - your-configuration-YAML-file", "");

        // create file to the working directory needed for the EncryptionReplacer
        createPasswordFile("password.txt", "h4z3lc4$t");

        Config xmlConfig = new InMemoryXmlConfig(fullExampleXml);
        Config yamlConfig = new InMemoryYamlConfig(fullExampleYaml);

        sortClientPermissionConfigs(xmlConfig);
        sortClientPermissionConfigs(yamlConfig);

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullExampleWithAdvancedNetwork() throws IOException {
        String fullExampleXml = readResourceToString("hazelcast-full-example.xml");
        String fullExampleYaml = readResourceToString("hazelcast-full-example.yaml");

        // remove imports to prevent the test from failing with importing non-existing files
        fullExampleXml = fullExampleXml.replace("<import resource=\"your-configuration-XML-file\"/>", "");
        fullExampleYaml = fullExampleYaml
                .replace("\r", "")
                .replace("import:\n    - your-configuration-YAML-file", "");

        // create file to the working directory needed for the EncryptionReplacer
        createPasswordFile("password.txt", "h4z3lc4$t");

        Config xmlConfig = new InMemoryXmlConfig(fullExampleXml);
        Config yamlConfig = new InMemoryYamlConfig(fullExampleYaml);

        // enabling advanced network configuration to compare the advanced
        // network config instead of the regular network configs
        xmlConfig.getAdvancedNetworkConfig().setEnabled(true);
        yamlConfig.getAdvancedNetworkConfig().setEnabled(true);

        sortClientPermissionConfigs(xmlConfig);
        sortClientPermissionConfigs(yamlConfig);

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    public static String readResourceToString(String resource) throws IOException {
        InputStream xmlInputStream = XmlYamlConfigBuilderEqualsTest.class.getClassLoader().getResourceAsStream(resource);
        return new String(IOUtil.toByteArray(xmlInputStream));
    }

    static File createPasswordFile(String passwordFileName, String passwordFileContent) throws IOException {
        File workDir = new File(".");
        File file = new File(workDir, passwordFileName);
        file.deleteOnExit();

        if (passwordFileContent != null && passwordFileContent.length() > 0) {
            PrintWriter out = new PrintWriter(file);
            try {
                out.print(passwordFileContent);
            } finally {
                IOUtil.closeResource(out);
            }
        }
        return file;
    }

    private void assertXmlYamlFileEquals(String filenameBase) {
        Config xmlConfig = new ClasspathXmlConfig(filenameBase + ".xml");
        Config yamlConfig = new ClasspathYamlConfig(filenameBase + ".yaml");

        sortClientPermissionConfigs(xmlConfig);
        sortClientPermissionConfigs(yamlConfig);

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    private void sortClientPermissionConfigs(Config config) {
        SecurityConfig securityConfig = config.getSecurityConfig();
        Set<PermissionConfig> unsorted = securityConfig.getClientPermissionConfigs();
        Set<PermissionConfig> sorted = new TreeSet<PermissionConfig>(new PermissionConfigComparator());
        sorted.addAll(unsorted);
        securityConfig.setClientPermissionConfigs(sorted);
    }

    private static class PermissionConfigComparator implements Comparator<PermissionConfig> {
        @Override
        public int compare(PermissionConfig o1, PermissionConfig o2) {
            if (o1 == o2) {
                return 0;
            }

            if (o1 == null) {
                return -1;
            }

            if (o2 == null) {
                return 1;
            }

            return o1.getType().name().compareTo(o2.getType().name());
        }
    }

}
