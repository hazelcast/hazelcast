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

package com.hazelcast.jet.core;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.test.SerialTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@Category({SerialTest.class})
public class JobUploadClientFailureTest extends JetTestSupport {

    @After
    public void resetSingleton() {
        // Reset the singleton after the test
        HazelcastBootstrap.resetSupplier();
    }

    @Test
    public void testNullJarPath() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void testNullJobParameters() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters();
        submitJobParameters.setJarPath(getJarPath());
        submitJobParameters.setJobParameters(null);

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }

    @Test
    public void testNullMainClass() {
        HazelcastInstance client = createResourceUploadEnabledMemberAndClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getNoManifestJarPath());

        assertThatThrownBy(() -> jetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("No Main-Class found in the manifest");
    }

    @Test
    public void testTooLongFileName() {
        HazelcastInstance client = createResourceUploadEnabledMemberAndClient();
        JetService jetService = client.getJet();
        JetClientInstanceImpl spyJetService = (JetClientInstanceImpl) Mockito.spy(jetService);

        Path jarPath = getJarPath();

        // Create a repeated filename
        char[] charArray = new char[300];
        Arrays.fill(charArray, 'a');
        String longFileName = new String(charArray);
        // JetClientInstanceImpl.getFileNameWithoutExtension return the long file name
        when(spyJetService.getFileNameWithoutExtension(jarPath)).thenReturn(longFileName);

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(jarPath);

        assertThatThrownBy(() -> spyJetService.submitJobFromJar(submitJobParameters))
                .isInstanceOf(JetException.class)
                .hasCauseInstanceOf(FileSystemException.class);
    }

    @Test
    public void test_jarUpload_whenResourceUploadIsNotEnabled() {
        createHazelcastInstance();
        HazelcastInstance client = createHazelcastClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath());

        assertThrows(JetException.class, () ->
                jetService.submitJobFromJar(submitJobParameters)
        );

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    @Test
    public void test_jarUpload_withWrongMainClassname() {
        HazelcastInstance client = createResourceUploadEnabledMemberAndClient();
        JetService jetService = client.getJet();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));
    }


    @Test
    public void test_jar_isDeleted() throws IOException {
        // Delete left over files before the test
        Path tempDirectory = Paths.get(System.getProperty("java.io.tmpdir"));
        String newSimpleJob = "newsimplejob.jar";
        fileAllLeftOverFiles(tempDirectory, newSimpleJob);

        HazelcastInstance client = createResourceUploadEnabledMemberAndClient();
        JetService jetService = client.getJet();

        // Copy as new jar to make it unique
        Path jarPath = getJarPath();
        Path newPath = jarPath.resolveSibling(newSimpleJob);
        Files.copy(jarPath, newPath, StandardCopyOption.REPLACE_EXISTING);

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(newPath)
                .setMainClass("org.example.Main1");

        assertThrows(JetException.class, () -> jetService.submitJobFromJar(submitJobParameters));

        boolean fileDoesNotExist = fileDoesNotExist(tempDirectory, newSimpleJob);
        assertThat(fileDoesNotExist)
                .isTrue();

        // Delete local new jar
        Files.delete(newPath);
    }

    private void fileAllLeftOverFiles(Path tempDirectory, String newJarName) throws IOException {
        try (Stream<Path> stream = Files.list(tempDirectory)) {
            stream.forEach(fullPath -> {
                String fileName = fullPath.getFileName().toString();
                if (fileName.contains(newJarName)) {
                    try {
                        Files.delete(fullPath);
                    } catch (IOException ignored) {
                    }
                }
                ;
            });
        }
    }

    private boolean fileDoesNotExist(Path tempDirectory, String newJarName) throws IOException {
        try (Stream<Path> stream = Files.list(tempDirectory)) {
            return stream.noneMatch(fullPath -> {
                String fileName = fullPath.getFileName().toString();
                return fileName.contains(newJarName);
            });
        }
    }

    @Test
    public void test_jarUpload_withIncorrectChecksum() throws IOException, NoSuchAlgorithmException {
        HazelcastInstance client = createResourceUploadEnabledMemberAndClient();

        // Mock the JetClientInstanceImpl to return an incorrect checksum
        JetClientInstanceImpl jetService = (JetClientInstanceImpl) client.getJet();
        JetClientInstanceImpl spyJetService = Mockito.spy(jetService);

        Path jarPath = getJarPath();
        when(spyJetService.calculateSha256Hex(jarPath)).thenReturn("1");
        List<String> jobParameters = emptyList();

        SubmitJobParameters submitJobParameters = new SubmitJobParameters()
                .setJarPath(getJarPath())
                .setJobParameters(jobParameters);

        assertThrows(JetException.class, () -> spyJetService.submitJobFromJar(submitJobParameters));

        assertEqualsEventually(() -> jetService.getJobs().size(), 0);
    }

    private HazelcastInstance createResourceUploadEnabledMemberAndClient() {
        Config config = smallInstanceConfig();
        JetConfig jetConfig = config.getJetConfig();
        jetConfig.setResourceUploadEnabled(true);

        createHazelcastInstance(config);
        return createHazelcastClient();
    }

    private Path getJarPath() {
        return getPath("simplejob-1.0.0.jar");
    }

    private Path getNoManifestJarPath() {
        return getPath("nomanifestsimplejob-1.0.0.jar");
    }

    private Path getPath(String jarName) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(jarName);
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + jarName);
        }
        return result;
    }
}
