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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.JarUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.jar.JarFile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ClassScannerTest {

    private static final byte[] DUMMY_CONTENT = "dummy-class-content".getBytes(UTF_8);
    private static Path dummyJarFile;

    @BeforeClass
    public static void setUp() throws Exception {
        dummyJarFile = Files.createTempFile("dummy-", ".jar");
        JarUtil.createJarFile(asList("com/example/SomeClass.class", "com/example/SomeOtherClass.class"), asList(DUMMY_CONTENT, DUMMY_CONTENT), dummyJarFile.toString());
    }

    @Test
    public void should_find_class_in_jar() throws IOException {
        assertThat(dummyJarFile).exists();

        try (JarFile jarFile = new JarFile(dummyJarFile.toFile())) {
            List<String> classFiles = ClassScanner.findClassFiles(jarFile, "SomeClass");

            String pathToClass = Paths.get("com", "example", "SomeClass.class").toString();
            assertThat(classFiles).contains(pathToClass);
        }
    }

    @Test
    public void should_NOT_find_class_in_jar() throws IOException {
        assertThat(dummyJarFile).exists();

        try (JarFile jarFile = new JarFile(dummyJarFile.toFile())) {
            List<String> classFiles = ClassScanner.findClassFiles(jarFile, "NonExistingClass");

            assertThat(classFiles).isEmpty();
        }
    }

    @Test
    public void should_find_class_file_in_classpath() {
        List<String> classFiles = ClassScanner.findClassFiles(HazelcastInstance.class);

        assertThat(classFiles).hasSize(1);
        assertThat(classFiles.get(0)).contains("com/hazelcast/core/HazelcastInstance.class");
    }

    @Test
    public void should_find_duplicated_class_files_in_classpath() throws Exception {
        URL someJarUrl = new URL("file:src/test/resources/ChildParent.jar");
        URL duplicatedJarUrl = duplicateJar(someJarUrl);
        URLClassLoader classLoader = new URLClassLoader(new URL[]{someJarUrl, duplicatedJarUrl}, getClass().getClassLoader());
        Class<?> someClass = Class.forName("usercodedeployment.ParentClass", true, classLoader);

        List<String> classFiles = ClassScanner.findClassFiles(someClass);

        assertThat(classFiles)
                .hasSize(2)
                .allMatch(filename -> filename.contains("usercodedeployment/ParentClass.class"));
    }

    private static URL duplicateJar(URL jarUrl) throws IOException {
        Path duplicateJarFile = Files.createTempFile("duplicate-", ".jar");
        Files.copy(Paths.get(jarUrl.getFile()), duplicateJarFile, StandardCopyOption.REPLACE_EXISTING);
        return new URL("file:" + duplicateJarFile);
    }

    @Test
    public void should_NOT_find_class_file_in_classpath() {
        List<String> classFiles = ClassScanner.findClassFiles(someClassWithoutClassFile());

        assertThat(classFiles).isEmpty();
    }

    private Class<?> someClassWithoutClassFile() {
        Object obj = Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), new Class[0], (proxy, method, args) -> method.invoke(args));
        return obj.getClass();
    }

    @AfterClass
    public static void afterClass() {
        IOUtil.delete(dummyJarFile.toFile());
    }
}
