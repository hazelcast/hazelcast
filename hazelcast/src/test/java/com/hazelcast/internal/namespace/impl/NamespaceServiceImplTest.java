/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.google.common.io.Files;
import com.google.common.net.UrlEscapers;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.NamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.annotation.NamespaceTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static com.hazelcast.test.UserCodeUtil.fileRelativeToBinariesFolder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Category(NamespaceTest.class)
public class NamespaceServiceImplTest {
    public static Stream<Arguments> testLoadClasses() throws IOException {
        return Stream.of(
                Arguments.of(
                        Named.of(ResourceType.CLASS.toString(),
                                classResourcesFromClassPath("usercodedeployment/ChildClass.class",
                                        "usercodedeployment/ParentClass.class")),
                        new String[] {"usercodedeployment.ParentClass", "usercodedeployment.ChildClass"}),
                Arguments.of(
                        Named.of(ResourceType.JAR.toString(),
                                singletonJarResourceFromBinaries("usercodedeployment/ChildParent.jar")),
                        new String[] {"usercodedeployment.ParentClass", "usercodedeployment.ChildClass"}),
                Arguments.of(
                        Named.of(ResourceType.JARS_IN_ZIP.toString(),
                                jarResourcesFromBinaries("/zip-resources/person-car-jar.zip")),
                        new String[] {"com.sample.pojo.car.Car"}));
    }

    @ParameterizedTest
    @MethodSource
    void testLoadClasses(Collection<ResourceDefinition> resources, String... expectedClasses) throws Exception {
        NamespaceServiceImpl namespaceService =
                new NamespaceServiceImpl(NamespaceServiceImplTest.class.getClassLoader(), Collections.emptyMap(), null);

        namespaceService.addNamespace("ns1", resources);
        ClassLoader classLoader = namespaceService.getClassLoaderForExactNamespace("ns1");

        for (String expectedClass : expectedClasses) {
            Class<?> clazz = classLoader.loadClass(expectedClass);

            // Workaround the fact that the "Car" class doesn't have a default constructor
            Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
            Object[] parameters = new Object[constructor.getParameterCount()];

            assertDoesNotThrow(() -> constructor.newInstance(parameters));
        }
    }

    private static Collection<ResourceDefinition> classResourcesFromClassPath(String... classIdPaths) {
        return Arrays.stream(classIdPaths).map(idPath -> {
            try {
                final byte[] bytes = Files.toByteArray(fileRelativeToBinariesFolder(idPath));
                return new ResourceDefinitionImpl(idPath, bytes, ResourceType.CLASS, idPath);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());
    }

    private static Collection<ResourceDefinition> singletonJarResourceFromBinaries(final String idPath) throws IOException {
        final byte[] bytes = Files.toByteArray(fileRelativeToBinariesFolder(idPath));
        return Collections.singleton(new ResourceDefinitionImpl(idPath, bytes, ResourceType.JAR, idPath));
    }

    private static Collection<ResourceDefinition> jarResourcesFromBinaries(final String idPath) throws IOException {
        try (InputStream stream = NamespaceServiceImplTest.class.getResource(idPath).openStream()) {
            return Collections
                    .singleton(new ResourceDefinitionImpl(idPath, stream.readAllBytes(), ResourceType.JARS_IN_ZIP, idPath));
        }
    }

    @Test
    void testXmlConfigDefinedFiltering_ClassBlacklist() {
        assertThrows(SecurityException.class, () -> testXmlConfigDefinedFiltering(
                "<class>usercodedeployment.ParentClass</class>", "<package>com.foo.bar</package>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PackageBlacklist() {
        assertThrows(SecurityException.class, () -> testXmlConfigDefinedFiltering(
                "<package>usercodedeployment</package>", "<package>com.foo.bar</package>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PrefixBlacklist() {
        assertThrows(SecurityException.class, () -> testXmlConfigDefinedFiltering(
                "<package>usercodedeployment.Child</package>", "<package>com.foo.bar</package>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PrefixBlacklist_NotApplicable() {
        assertDoesNotThrow(() -> testXmlConfigDefinedFiltering(
                "<package>com.foo.bar</package>", null));
    }

    @Test
    void testXmlConfigDefinedFiltering_ClassWhitelist() {
        assertDoesNotThrow(() -> testXmlConfigDefinedFiltering(
                "<package>com.foo.bar</package>", "<class>usercodedeployment.ParentClass</class>\n"
                        + "<class>usercodedeployment.ChildClass</class>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PackageWhitelist() {
        assertDoesNotThrow(() -> testXmlConfigDefinedFiltering(
                "<package>com.foo.bar</package>", "<package>usercodedeployment</package>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PrefixWhitelist_NotMatching() {
        assertThrows(SecurityException.class, () -> testXmlConfigDefinedFiltering(
                null, "<package>com.foo.bar</package>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_PrefixWhitelist() {
        assertDoesNotThrow(() -> testXmlConfigDefinedFiltering(
                "<package>com.foo.bar</package>", "<prefix>usercodedeployment.</prefix>"));
    }

    @Test
    void testXmlConfigDefinedFiltering_NoneDefined() {
        assertDoesNotThrow(() -> testXmlConfigDefinedFiltering(
                null, null));
    }

    // This could be programmatic in the future, but serves its purpose as-is
    private void testXmlConfigDefinedFiltering(String blacklistLine, String whitelistLine) {
        String stringPath =
                getCorrectedPathString(Paths.get("src", "test", "class", "usercodedeployment", "ChildParent.jar"));

        String xmlPayload = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"\n"
                + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                + "           xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n"
                + "           http://www.hazelcast.com/schema/config/hazelcast-config-5.4.xsd\">\n" + "\n"
                + "    <cluster-name>cluster</cluster-name>\n\n"
                + "    <namespaces enabled=\"true\">\n"
                + "      <java-serialization-filter defaults-disabled=\"true\">\n"
                + (blacklistLine == null ? ""
                : "          <blacklist>\n"
                + "              " + blacklistLine + "\n"
                + "          </blacklist>\n")
                + (whitelistLine == null ? ""
                : "          <whitelist>\n"
                + "              " + whitelistLine + "\n"
                + "          </whitelist>\n")
                + "      </java-serialization-filter>"
                + "      <namespace name=\"myNamespace\">\n"
                + "          <jar>\n"
                + "              <url>file:///" + stringPath + "</url>\n"
                + "          </jar>\n"
                + "      </namespace>\n"
                + "    </namespaces>\n"
                + "</hazelcast>\n" + "\n";

        // Start an instance & confirm that our namespaces were loaded
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(Config.loadFromString(xmlPayload));
        try {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            NamespaceService service = nodeEngine.getNamespaceService();
            assertTrue(service.isEnabled());
            assertTrue(service.hasNamespace("myNamespace"));
        } finally {
            instance.shutdown();
        }
    }

    private String getCorrectedPathString(Path path) {
        return OsHelper.ensureUnixSeparators(UrlEscapers.urlFragmentEscaper().escape(path.toAbsolutePath().toString()));
    }

    // "No-op" implementation test
    @Test
    void testNoOpImplementation() {
        // Do not enable Namespaces in any form, results in No-Op implementation being used
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
        try {
            NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
            NamespaceService service = nodeEngine.getNamespaceService();
            assertFalse(service.isEnabled());
            assertTrue(service instanceof NoOpNamespaceService);
            assertFalse(nodeEngine.getConfigClassLoader() instanceof NamespaceAwareClassLoader);
            assertFalse(service.isDefaultNamespaceDefined());
        } finally {
            instance.shutdown();
        }
    }
}
