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
import com.hazelcast.config.DiagnosticsConfig;
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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiagnosticsLogFileTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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
    public void testDiagnosticsDirectoryIsCreatedWhenDoesNotExistOverConfig() throws IOException {
        Config config = new Config();
        File parentFolder = folder.newFolder();

        // this directory does not exist, we want Hazelcast to create it
        File diagnosticsFolder = new File(parentFolder, "newdir");

        config.getDiagnosticsConfig().setEnabled(true);
        config.getDiagnosticsConfig().setLogDirectory(diagnosticsFolder.getAbsolutePath());

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
    public void testDiagnosticsFilesRollOver() throws IOException, NoSuchFieldException, IllegalAccessException {

        // Prepare the config for a lot of output
        Config config = new Config();
        File diagnosticsFolder = folder.newFolder();
        DiagnosticsConfig diagnosticsConfig = new DiagnosticsConfig()
                .setEnabled(true)
                .setMaxRolledFileCount(5)
                .setMaxRolledFileSizeInMB(1)
                .setLogDirectory(diagnosticsFolder.getAbsolutePath())
                .setFileNamePrefix("my-prefix")
                .setProperty(OperationProfilerPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(SlowOperationPlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(InvocationProfilerPlugin.PERIOD_SECONDS.getName(), "1");

        config.setDiagnosticsConfig(diagnosticsConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        // Let's tweak the file size on LogFileWrite to speed up the test.
        Diagnostics diagnostics = TestUtil.getNode(instance).getNodeEngine().getDiagnostics();
        DiagnosticsLogFile fileLogger = (DiagnosticsLogFile) diagnostics.diagnosticsLog;
        Field field = DiagnosticsLogFile.class.getDeclaredField("maxRollingFileSizeBytes");
        field.setAccessible(true);
        field.set(fileLogger, 10_000); // 10 kilobyte
        String fileNameOfFirstRun = diagnostics.getFileName();

        // Ensure configured amount of files are created
        assertContainsFileEventually(diagnosticsFolder, diagnosticsConfig.getMaxRolledFileCount());

        // now, let's decrease the rolling count. Since each run on diagnostics is a different "session",
        // previous files should be there

        // from first run, there should be 5 files
        int totalExpectedFileCount = diagnosticsConfig.getMaxRolledFileCount();
        diagnosticsConfig.setMaxRolledFileCount(2);
        // from second run, there should be 2 files -> we expect 7 files
        totalExpectedFileCount += diagnosticsConfig.getMaxRolledFileCount();

        // set the config for second phase
        instance.getConfig().setDiagnosticsConfig(diagnosticsConfig);
        // let's tweak the file size on LogFileWrite to speed up the test.
        fileLogger = (DiagnosticsLogFile) diagnostics.diagnosticsLog;
        field.set(fileLogger, 10_000); // 10 kilobyte
        assertContainsFileEventually(diagnosticsFolder, totalExpectedFileCount);


        File[] logFiles = diagnosticsFolder.listFiles();
        String fileNameOfSecondRun = diagnostics.getFileName();

        // There should be files from two different "sessions" or runs
        assertNotEquals(fileNameOfFirstRun, fileNameOfSecondRun);
        List<String> fileNames = Arrays.stream(logFiles).map((file) -> file.getName()).toList();
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
}
