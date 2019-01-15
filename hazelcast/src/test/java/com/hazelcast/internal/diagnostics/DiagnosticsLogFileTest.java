package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        config.setProperty("hazelcast.diagnostics.metric.level", "DEBUG");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        createHazelcastInstance(config);
        assertContainsFileEventually(diagnosticsFolder);

    }

    @Test
    public void testDiagnosticsFileCreatedWhenDirectoryExist() throws IOException {
        Config config = new Config();
        File diagnosticsFolder = folder.newFolder();

        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.metric.level", "DEBUG");
        config.setProperty("hazelcast.diagnostics.directory", diagnosticsFolder.getAbsolutePath());

        createHazelcastInstance(config);
        assertContainsFileEventually(diagnosticsFolder);

    }

    private void assertContainsFileEventually(final File dir) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(dir.exists());
                assertTrue(dir.isDirectory());

                File[] files = dir.listFiles();
                assertTrue(files.length > 0);
            }
        });
    }
}