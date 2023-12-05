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

package com.hazelcast.instance.impl;

import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.test.annotation.NamespaceTest;
import com.hazelcast.test.starter.MavenInterface;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.test.UserCodeUtil.fileRelativeToBinariesFolder;
import static com.hazelcast.test.UserCodeUtil.urlFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Test static namespace configuration, resource resolution and classloading end-to-end */
@Category(NamespaceTest.class)
public class NamespaceAwareClassLoaderTest extends ConfigClassLoaderTest {
    private static Path classRoot;
    private static MapResourceClassLoader mapResourceClassLoader;
    private static Artifact h2V202Artifact;

    @BeforeClass
    public static void setUpClass() throws IOException {
        classRoot = Paths.get("src/test/class");
        mapResourceClassLoader = generateMapResourceClassLoaderForDirectory(classRoot);
        h2V202Artifact = new DefaultArtifact("com.h2database", "h2", null, "2.0.202");
    }

    @Before
    public void prepareConfig() {
        config.getNamespacesConfig().setEnabled(true);
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoaded() throws Exception {
        createHazelcastInstanceWithConfig();
        Class<?> klass = tryLoadClass(null, "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenLoadClassKnownToParent_thenIsLoadedWithNoNamespaceDefined() throws Exception {
        createHazelcastInstanceWithConfig();
        Class<?> klass = tryLoadClass(null, "com.hazelcast.core.HazelcastInstance");
        assertSame(HazelcastInstance.class, klass);
    }

    @Test
    public void whenSimpleClassInNs_thenIsLoaded() throws Exception {
        config.getNamespacesConfig().addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass")));
         createHazelcastInstanceWithConfig();

        Class<?> parentClass = tryLoadClass("ns1", "usercodedeployment.ParentClass");
        assertInstanceOf(MapResourceClassLoader.class, parentClass.getClassLoader());
    }

    @Test
    public void whenSimpleClassInNs_thenIsNotLoadedWithNoNamespaceDefined() throws Exception {
        config.getNamespacesConfig().addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass")));
        createHazelcastInstanceWithConfig();

        assertThrows(ClassNotFoundException.class, () -> tryLoadClass(null, "usercodedeployment.ParentClass"));
    }

    @Test
    public void whenClassWithHierarchyInNs_thenIsLoaded() throws Exception {
        config.getNamespacesConfig().addNamespaceConfig(
                new NamespaceConfig("ns1").addClass(mapResourceClassLoader.loadClass("usercodedeployment.ParentClass"))
                        .addClass(mapResourceClassLoader.loadClass("usercodedeployment.ChildClass")));
         createHazelcastInstanceWithConfig();

        Class<?> childClass = tryLoadClass("ns1", "usercodedeployment.ChildClass");
        assertEquals("usercodedeployment.ParentClass", childClass.getSuperclass().getName());
        // assert child & parent are loaded by same classloader
        assertSame(childClass.getClassLoader(), childClass.getSuperclass().getClassLoader());
    }

    @Test
    public void whenLoadInnerClassKnownToParent_thenIsLoaded() throws Exception {
        config.getNamespacesConfig().addNamespaceConfig(new NamespaceConfig("ns1").addJar(
                urlFromFile(fileRelativeToBinariesFolder("/usercodedeployment/EntryProcessorWithAnonymousAndInner.jar")), null));
         createHazelcastInstanceWithConfig();

        tryLoadClass("ns1", "usercodedeployment.EntryProcessorWithAnonymousAndInner");
        tryLoadClass("ns1", "usercodedeployment.EntryProcessorWithAnonymousAndInner$Test");
    }

    private static int countSqlDriversOf(ClassLoader classLoader) {
        Enumeration<URL> urls = Util.uncheckCall(() -> classLoader.getResources("META-INF/services/java.sql.Driver"));
        int count = 0;
        while (urls.hasMoreElements()) {
            count++;
            urls.nextElement();
        }
        return count;
    }

    @Test
    public void testServiceLoader_whenMultipleServicesOnClasspath() throws Exception {
        config.getNamespacesConfig().addNamespaceConfig(
                new NamespaceConfig("ns1").addJar(MavenInterface.locateArtifact(h2V202Artifact).toUri().toURL(), null));
        createHazelcastInstanceWithConfig();
        ClassLoader testClassLoader = NamespaceAwareClassLoaderTest.class.getClassLoader();

        // We need to provide NodeEngine context (as would be in place for Operations normally)
        NamespaceUtil.runWithNamespace("ns1", () -> {
            int countInTestClasspath = countSqlDriversOf(testClassLoader);
            int countInNamespace = countSqlDriversOf(engineConfigClassLoader);
            // namespace classpath contains one additional URL to META-INF/services/java.sql.Driver
            // in the H2 JDBC driver artifact added to the namespace
            assertEquals(countInTestClasspath + 1, countInNamespace);

            ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class, testClassLoader);
            Driver h2DriverFromTestCP = null;
            for (Driver d : loader) {
                if (d.getClass().getName().equals("org.h2.Driver")) {
                    h2DriverFromTestCP = d;
                }
            }
            // verify driver version loaded from test classpath
            assertSame(testClassLoader, h2DriverFromTestCP.getClass().getClassLoader());
            assertEquals(2, h2DriverFromTestCP.getMajorVersion());
            assertEquals(2, h2DriverFromTestCP.getMinorVersion());

            Driver h2DriverFromNamespaceCP = null;
            loader = ServiceLoader.load(Driver.class, engineConfigClassLoader);
            List<ServiceLoader.Provider<Driver>> providers =
                    loader.stream().filter(d -> d.type().getName().equals("org.h2.Driver")).collect(Collectors.toList());
            assertEquals(1, providers.size());
            h2DriverFromNamespaceCP = providers.get(0).get();
            // verify driver is loaded from older version that is present in namespace classpath
            assertInstanceOf(MapResourceClassLoader.class, h2DriverFromNamespaceCP.getClass().getClassLoader());
            assertEquals(2, h2DriverFromNamespaceCP.getMajorVersion());
            assertEquals(0, h2DriverFromNamespaceCP.getMinorVersion());
        });
    }

    /** Find & load all .class files in the scope of this test */
    private static MapResourceClassLoader generateMapResourceClassLoaderForDirectory(Path root) throws IOException {
        try (Stream<Path> stream = Files.walk(root.resolve("usercodedeployment"))) {
            final Map<String, byte[]> classNameToContent =
                    stream.filter(path -> FilenameUtils.isExtension(path.getFileName().toString(), "class"))
                            .collect(Collectors.toMap(path -> JobRepository.classKeyName(root.relativize(path).toString()), path -> {
                                try {
                                    return IOUtil.compress(Files.readAllBytes(path));
                                } catch (final IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }));

            return new MapResourceClassLoader(null, NamespaceAwareClassLoaderTest.class.getClassLoader(), () -> classNameToContent,
                    true);
        }
    }

    Class<?> tryLoadClass(String namespace, String className) throws Exception {
        if (namespace != null) {
            NamespaceUtil.setupNamespace(engine, namespace);
        }
        try {
            return engineConfigClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(
                    MessageFormat.format("\"{0}\" class not found in \"{1}\" namespace", className, namespace), e);
        } finally {
            if (namespace != null) {
                NamespaceUtil.cleanupNamespace(engine, namespace);
            }
        }
    }
}
