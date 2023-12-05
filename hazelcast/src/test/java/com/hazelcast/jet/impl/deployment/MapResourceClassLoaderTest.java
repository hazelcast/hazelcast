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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.UserCodeUtil;
import com.hazelcast.test.annotation.NamespaceTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.compress;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.jet.impl.util.ReflectionUtils.toClassResourceId;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Category(NamespaceTest.class)
class MapResourceClassLoaderTest {
    private Map<String, byte[]> classBytes = new HashMap<>();
    private MapResourceClassLoader classLoader;
    private ClassLoader parentClassLoader;

    @BeforeEach
    public void setup() throws IOException {
        parentClassLoader = this.getClass().getClassLoader();
        loadClassesFromJar("usercodedeployment/ChildParent.jar");
        loadClassesFromJar("usercodedeployment/IncrementingEntryProcessor.jar");
        loadClassesFromJar("usercodedeployment/ShadedClasses.jar");
    }

    @Test
    void findClass_whenClassFromMap() throws Exception {
        classLoader = new MapResourceClassLoader(null, null, () -> classBytes, false);
        assertDoesNotThrow(
                () -> classLoader.findClass("usercodedeployment.ParentClass").getDeclaredConstructor().newInstance());
        assertDoesNotThrow(() -> classLoader.findClass("usercodedeployment.ChildClass").getDeclaredConstructor().newInstance());
    }

    @Test
    void findClass_whenClassFromMapReferencesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, false);
        // IncrementingEntryProcessor implements EntryProcessor
        assertDoesNotThrow(() -> classLoader.findClass("usercodedeployment.IncrementingEntryProcessor").getDeclaredConstructor()
                .newInstance());
    }

    @Test
    void loadClass_whenClassFromParentClassLoader() throws Exception {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, false);
        assertDoesNotThrow(() -> classLoader.loadClass("com.hazelcast.map.EntryProcessor"));
    }

    @Test
    void loadClassChildFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, true);
        // com.hazelcast.core.HazelcastInstance loaded from ShadedClasses.jar is a concrete class with a main method
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        assertFalse(klass.isInterface());
    }

    @Test
    void loadClassParentFirst_whenClassFromChild_shadesClassFromParent() throws Exception {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, false);
        // expect to load com.hazelcast.core.HazelcastInstance interface from the codebase
        Class<?> klass = classLoader.loadClass("com.hazelcast.core.HazelcastInstance");
        assertTrue(klass.isInterface());
    }

    @Test
    void getResource_whenResolvableFromChild() {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, true);
        URL url = classLoader.getResource("usercodedeployment/ParentClass.class");
        assertEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
        assertEquals(toClassResourceId("usercodedeployment.ParentClass"), url.getFile());
    }

    @Test
    void getResource_whenResolvableFromParent() {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, true);
        URL url = classLoader.getResource("com/hazelcast/map/IMap.class");
        assertNotNull(url);
        assertNotEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
    }

    @Test
    void getResource_whenResolvableFromChild_andNotChildFirst() {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, false);
        URL url = classLoader.getResource("usercodedeployment/ParentClass.class");
        assertEquals(MapResourceClassLoader.PROTOCOL, url.getProtocol());
        assertEquals(toClassResourceId("usercodedeployment.ParentClass"), url.getFile());
    }

    static Stream<Arguments> findResource_negativeCases() {
        return Stream.of(Arguments.of(Named.of("Empty String", StringUtils.EMPTY)),
                Arguments.of(Named.of("findResource is meant to only search in this classloader's resources, not the parent",
                        "com/hazelcast/map/IMap.class")));
    }

    @ParameterizedTest
    @NullSource
    @MethodSource("findResource_negativeCases")
    void findResource_negativeCases(String name) {
        classLoader = new MapResourceClassLoader(null, parentClassLoader, () -> classBytes, true);
        URL url = classLoader.findResource(name);
        assertNull(url);
    }

    private void loadClassesFromJar(String jarPath) throws IOException {
        JarInputStream inputStream = null;
        try {
            inputStream = getJarInputStream(jarPath);
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                String className = ClassLoaderUtil.extractClassName(entry.getName());
                if (className == null) {
                    continue;
                }
                byte[] classDefinition = compress(inputStream.readAllBytes());
                inputStream.closeEntry();
                classBytes.put(JobRepository.classKeyName(toClassResourceId(className)), classDefinition);
            } while (true);
        } finally {
            closeResource(inputStream);
        }
    }

    private JarInputStream getJarInputStream(String jarPath) throws IOException {
        File file = UserCodeUtil.fileRelativeToBinariesFolder(jarPath);
        if (file.exists()) {
            return new JarInputStream(new FileInputStream(file));
        }

        try {
            return new JarInputStream(new URL(jarPath).openStream());
        } catch (MalformedURLException e) {
            ignore(e);
        }

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(jarPath);
        if (inputStream == null) {
            throw new FileNotFoundException("File could not be found in " + jarPath + "  and resources/" + jarPath);
        }
        return new JarInputStream(inputStream);
    }
}
