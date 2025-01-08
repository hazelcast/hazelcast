/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class XmlYamlConfigBuilderEqualsTest extends HazelcastTestSupport {
    private Path tempDirectory;

    @Before
    public void setUp() throws IOException {
        tempDirectory = Files.createTempDirectory(getClass().getName());
        tempDirectory.toFile().deleteOnExit();
    }

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
        Config xmlConfig = getXML("hazelcast-full-example.xml");
        Config yamlConfig = getYaml("hazelcast-full-example.yaml");

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    @Test
    public void testFullExampleWithAdvancedNetwork() throws IOException {
        Config xmlConfig = getXML("hazelcast-full-example.xml");
        Config yamlConfig = getYaml("hazelcast-full-example.yaml");

        // enabling advanced network configuration to compare the advanced
        // network config instead of the regular network configs
        xmlConfig.getAdvancedNetworkConfig().setEnabled(true);
        yamlConfig.getAdvancedNetworkConfig().setEnabled(true);

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    public static String readResourceToString(String resource) throws IOException {
        try (InputStream xmlInputStream = XmlYamlConfigBuilderEqualsTest.class.getClassLoader().getResourceAsStream(resource)) {
            assert xmlInputStream != null;
            return new String(xmlInputStream.readAllBytes(), UTF_8);
        }
    }

    private Config getXML(String resource) throws IOException {
        String xml = readResourceToString(resource);

        // remove imports to prevent the test from failing with importing non-existing files
        xml = StringUtils.remove(xml, "<import resource=\"your-configuration-XML-file\"/>");

        xml = replaceExampleValuesWithRealFiles(xml);

        Config config = new InMemoryXmlConfig(xml);
        sortClientPermissionConfigs(config);

        return config;
    }

    private Config getYaml(String resource) throws IOException {
        String yaml = readResourceToString(resource);

        // remove imports to prevent the test from failing with importing non-existing files
        yaml = StringUtils.remove(yaml, "\r");
        yaml = StringUtils.remove(yaml, "import:\n    - your-configuration-YAML-file");

        yaml = replaceExampleValuesWithRealFiles(yaml);

        Config config = new InMemoryYamlConfig(yaml);
        sortClientPermissionConfigs(config);

        return config;
    }

    /** Some parts of the example file don't actually resolve, which needs to be post-processed before inflation */
    private String replaceExampleValuesWithRealFiles(String str) throws IOException {
        str = replacePasswordFileWithTemporaryFile(str);
        str = replaceUCNReferencesWithTemporaryFile(str);

        return str;
    }

    /**
     * The supplied config files contain a {@value EncryptionReplacer#PROPERTY_PASSWORD_FILE} reference to a path of
     * {@code password.txt}, which is resolved at load-time by {@link EncryptionReplacer} - if missing, an exception is thrown.
     * <p>
     * This file doesn't exist within the scope of the test, especially not in the working directory - so create a temporary
     * file, and update the reference to use that instead.
     */
    private static String replacePasswordFileWithTemporaryFile(String str) throws IOException {
        final Path file = Files.createTempFile("password", ".txt");
        file.toFile().deleteOnExit();

        Files.writeString(file, "h4z3lc4$t");

        return str.replace("password.txt", file.toString());
    }

    /** Replace the UCN example paths with real files (but empty) to ensure they can be inflated */
    private String replaceUCNReferencesWithTemporaryFile(String str) throws IOException {
        for (Path pathToReplace : new Path[] {Path.of("etc", "hazelcast", "jar.jar"), Path.of("etc", "hazelcast", "jar2.jar"),
                Path.of("etc", "hazelcast", "jar3.jar"), Path.of("etc", "hazelcast", "jarsInZip.zip"),
                Path.of("etc", "hazelcast", "classes", "MyClass.class")}) {

            Path newPath = tempDirectory.resolve(pathToReplace);

            try {
                Files.createDirectories(newPath.getParent());
                Files.createFile(newPath);
            } catch (FileAlreadyExistsException ignored) {
            }

            // Convert the Path to the URL equivalent we expect to be present in the config file

            // It's not possible to do this using the typical "Path.toUri()" approach
            // On Windows this implicitly converts the path to an absolute path first, meaning you get a
            // "file:///C:/path/to/the/repo/etc" URL
            // Even if you create an absolute path to nowhere, Windows will still assume it's relative to the C: drive and you
            // end up with "file:///C:/etc" URL
            // For tests where the scope is controlled and no escaping is required, hardcoding is sufficient
            str = str.replace("file:///" + OS.ensureUnixSeparators(pathToReplace.toString()), newPath.toUri()
                    .toString());
        }

        return str;
    }

    private static void assertXmlYamlFileEquals(String filenameBase) {
        Config xmlConfig = new ClasspathXmlConfig(filenameBase + ".xml");
        Config yamlConfig = new ClasspathYamlConfig(filenameBase + ".yaml");

        sortClientPermissionConfigs(xmlConfig);
        sortClientPermissionConfigs(yamlConfig);

        String xmlConfigFromXml = new ConfigXmlGenerator(true).generate(xmlConfig);
        String xmlConfigFromYaml = new ConfigXmlGenerator(true).generate(yamlConfig);

        assertEquals(xmlConfigFromXml, xmlConfigFromYaml);
    }

    private static void sortClientPermissionConfigs(Config config) {
        SecurityConfig securityConfig = config.getSecurityConfig();
        Set<PermissionConfig> unsorted = securityConfig.getClientPermissionConfigs();
        Set<PermissionConfig> sorted = new TreeSet<>(new PermissionConfigComparator());
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
