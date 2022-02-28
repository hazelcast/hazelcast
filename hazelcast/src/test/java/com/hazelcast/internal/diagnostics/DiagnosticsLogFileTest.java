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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
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
    public void testDiagnosticsFileCreatedWhenDirectoryExist() throws IOException {
        Config config = new Config();
        File diagnosticsFolder = folder.newFolder();

        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        createHazelcastInstance(config);
        assertContainsFileEventually(diagnosticsFolder);

    }

    private void assertContainsFileEventually(final File dir) {
        assertTrueEventually(() -> {
            assertTrue(dir.exists());
            assertTrue(dir.isDirectory());

            File[] files = dir.listFiles();
            assertNotNull(files);
            assertTrue(files.length > 0);
        });
    }
}
