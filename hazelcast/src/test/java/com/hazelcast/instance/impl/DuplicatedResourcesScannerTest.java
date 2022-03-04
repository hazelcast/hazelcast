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

import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.JarUtil;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DuplicatedResourcesScannerTest {

    private static final byte[] SOME_CONTENT = "some-content".getBytes(UTF_8);
    private static final String SOME_EXISTING_RESOURCE_FILE = "META-INF/some-resource-file";
    private final ILogger logger = mock(ILogger.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path dummyJarFile;

    @Before
    public void setUp() throws Exception {
        dummyJarFile = temporaryFolder.newFile("dummy.jar").toPath();
        JarUtil.createJarFile(singletonList(SOME_EXISTING_RESOURCE_FILE), singletonList(SOME_CONTENT), dummyJarFile.toString());
    }

    @Test
    public void should_NOT_log_warning_when_single_occurrence() throws Exception {
        URLClassLoader classLoader = classLoaderWithJars(dummyJarFile);

        DuplicatedResourcesScanner.checkForDuplicates(classLoader, logger, SOME_EXISTING_RESOURCE_FILE);

        verifyNoInteractions(logger);
    }

    @Test
    public void should_log_warning_when_duplicate_found() throws Exception {
        URLClassLoader classLoader = classLoaderWithJars(dummyJarFile, duplicateJar(dummyJarFile));

        DuplicatedResourcesScanner.checkForDuplicates(classLoader, logger, SOME_EXISTING_RESOURCE_FILE);

        ArgumentCaptor<String> logCaptor = ArgumentCaptor.forClass(String.class);
        verify(logger).warning(logCaptor.capture());
        assertThat(logCaptor.getValue()).contains("WARNING: Classpath misconfiguration: found multiple " + SOME_EXISTING_RESOURCE_FILE);
    }

    private URLClassLoader classLoaderWithJars(Path... jarFiles) throws IOException {
        List<URL> urls = new ArrayList<>();
        for (Path jarFile : jarFiles) {
            urls.add(jarFile.toUri().toURL());
        }
        return new URLClassLoader(urls.toArray(new URL[0]), getClass().getClassLoader());
    }

    private Path duplicateJar(Path jarFile) throws IOException {
        File duplicateJarFile = temporaryFolder.newFile("duplicate.jar");
        Files.copy(jarFile, duplicateJarFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return duplicateJarFile.toPath();
    }

    @Test
    public void should_NOT_log_warning_when_no_occurrence() {
        DuplicatedResourcesScanner.checkForDuplicates(getClass().getClassLoader(), logger, "META-INF/some-non-existing-file");

        verifyNoInteractions(logger);
    }

}
