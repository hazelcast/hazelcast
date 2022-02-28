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

package com.hazelcast.instance.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.JarUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.jar.JarFile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class JarScannerTest {

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
            List<String> classFiles = JarScanner.findClassFiles(jarFile, "SomeClass");

            String pathToClass = Paths.get("com", "example", "SomeClass.class").toString();
            assertThat(classFiles).contains(pathToClass);
        }
    }

    @Test
    public void should_NOT_find_class_in_jar() throws IOException {
        assertThat(dummyJarFile).exists();

        try (JarFile jarFile = new JarFile(dummyJarFile.toFile())) {
            List<String> classFiles = JarScanner.findClassFiles(jarFile, "NonExistingClass");

            assertThat(classFiles).isEmpty();
        }
    }

    @AfterClass
    public static void afterClass() {
        IOUtil.delete(dummyJarFile.toFile());
    }
}
