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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsLogFileTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryFolder restrictedFolder = new TemporaryFolder();

    @Test
    public void testDiagnosticsDirectoryIsCreatedWhenDoesNotExist() throws IOException {
        Config config = new Config();
        File parentFolder = folder.newFolder();

        // this directory does not exist, we want Hazelcast to create it
        File diagnosticsFolder = new File(parentFolder, "newdir");

        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        createHazelcastInstance(config);
        assertContainsFileEventually(diagnosticsFolder);
    }

    @Test
    public void testDiagnosticsFileCreatedWhenDirectoryExist() throws IOException {
        Config config = new Config();
        File diagnosticsFolder = folder.newFolder();

        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        createHazelcastInstance(config);
        assertContainsFileEventually(diagnosticsFolder);

    }

    @Test
    public void testDiagnosticsFilesRollsOver() throws IOException {
        File diagnosticsFolder = folder.newFolder();
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setMaxRolledFileCount(5)
                .setMaxRolledFileSizeInMB(0.01f)
                .setLogDirectory(diagnosticsFolder.getAbsolutePath())
                .setFileNamePrefix("my-prefix")
                .setAutoOffDurationInMinutes(5)
                .setProperty(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(SlowOperationPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(InvocationProfilerPlugin.PERIOD_SECONDS.getName(), "1");

        assertFilesRollingOver(diagnosticsConfig);

    }

    @Test
    public void testDiagnosticsFilesRollsOverWithDefaultConfig() throws IOException {
        File diagnosticsFolder = folder.newFolder();
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setLogDirectory(diagnosticsFolder.getAbsolutePath())
                .setMaxRolledFileSizeInMB(0.01f);

        assertFilesRollingOver(diagnosticsConfig);
    }

    @Test
    public void testDiagnosticsFileFailsIfNoWritePermissions_dynamically() throws IOException {
        // file permissions are not supported on Windows, so we skip this test
        assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));

        File diagnosticsFolder = restrictedFolder.newFolder();
        // remove write permissions
        makeitReadonly(diagnosticsFolder);

        assert !diagnosticsFolder.canWrite();

        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setLogDirectory(diagnosticsFolder.getAbsolutePath())
                .setMaxRolledFileSizeInMB(0.01f);

        HazelcastInstance instance = createHazelcastInstance(new Config());
        Diagnostics diagnostics = TestUtil.getNode(instance).getNodeEngine().getDiagnostics();

        assertThrows(InvalidConfigurationException.class, () -> {
            diagnostics.setConfig(diagnosticsConfig);
        });
    }

    @Test
    public void testDiagnosticsFileFailsIfNoWritePermissions_statically() throws IOException {
        // file permissions are not supported on Windows, so we skip this test
        assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));

        File diagnosticsFolder = restrictedFolder.newFolder();
        // remove write permissions
        makeitReadonly(diagnosticsFolder);

        Config config = new Config();
        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        assertThrows(InvalidConfigurationException.class, () -> {
            createHazelcastInstance(config);
        });
    }


    public void assertFilesRollingOver(DiagnosticsConfig diagnosticsConfig) {

        // Prepare the config for a lot of output
        Config config = new Config();

        HazelcastInstance instance = createHazelcastInstance(config);

        Diagnostics diagnostics = TestUtil.getNode(instance).getNodeEngine().getDiagnostics();
        diagnostics.setConfig(diagnosticsConfig);

        String fileNameOfFirstRun = diagnostics.getFileName();

        // Ensure configured amount of files are created
        assertContainsFileEventually(diagnostics.getLoggingDirectory(), diagnosticsConfig.getMaxRolledFileCount());

        // now, let's decrease the rolling count. Since each run on diagnostics is a different "session",
        // previous files should be there

        // from first run, there should be 5 files
        int totalExpectedFileCount = diagnosticsConfig.getMaxRolledFileCount();
        diagnosticsConfig.setMaxRolledFileCount(2);
        // from second run, there should be 2 files -> we expect 7 files
        totalExpectedFileCount += diagnosticsConfig.getMaxRolledFileCount();

        // set the config for second phase
        diagnostics.setConfig(diagnosticsConfig);
        assertContainsFileEventually(diagnostics.getLoggingDirectory(), totalExpectedFileCount);


        File[] logFiles = diagnostics.getLoggingDirectory().listFiles();
        String fileNameOfSecondRun = diagnostics.getFileName();

        // There should be files from two different "sessions" or runs
        assertNotEquals(fileNameOfFirstRun, fileNameOfSecondRun);
        assert logFiles != null;
        List<String> fileNames = Arrays.stream(logFiles).map(File::getName).toList();
        assertTrue(fileNames.stream().anyMatch(fileName -> fileName.contains(fileNameOfFirstRun)));
        assertTrue(fileNames.stream().anyMatch(fileName -> fileName.contains(fileNameOfSecondRun)));
    }


    private void assertContainsFileEventually(final File dir) {
        assertContainsFileEventually(dir, 1);
    }

    private void assertContainsFileEventually(final File dir, int minFileCount) {
        assertTrueEventually(() -> {
            assertTrue(dir.exists());
            assertTrue(dir.isDirectory());

            File[] files = dir.listFiles();
            assertNotNull(files);
            assertTrue(files.length >= minFileCount);
        });
    }

    private void makeitReadonly(File readOnlyDir) {
        Path readOnlyPath = readOnlyDir.toPath();

        try {
            // UNIX-like systems: set Posix permissions
            Files.setPosixFilePermissions(readOnlyPath, PosixFilePermissions.fromString("r-xr-xr-x"));
        } catch (UnsupportedOperationException e) {
            // Windows: set as read-only
            readOnlyDir.setReadOnly();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
