/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.python;

import com.hazelcast.config.Config;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static com.hazelcast.jet.python.PythonTransforms.mapUsingPython;
import static com.hazelcast.jet.python.PythonTransforms.mapUsingPythonBatch;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PythonServiceTest extends SimpleTestInClusterSupport {

    private static final int ITEM_COUNT = 10_000;
    private static final String ECHO_HANDLER_FUNCTION =
            "def handle(input_list):\n" +
            "    return ['echo-%s' % i for i in input_list]\n";

    private static final String FAILING_FUNCTION
            = "def handle(input_list):\n"
            + "    assert 1 == 2\n";

    private File baseDir;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceWithResourceUploadConfig();
        initialize(1, config);
        assumeThatNoWindowsOS();
    }

    @Before
    public void before() throws Exception {
        baseDir = createTempDirectory();
        installFileToBaseDir(ECHO_HANDLER_FUNCTION, "echo.py");
    }

    @After
    public void after() {
        com.hazelcast.internal.nio.IOUtil.delete(baseDir);
    }

    @Test
    @Category({QuickTest.class, ParallelJVMTest.class})
    public void batchStage_mapUsingPython() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("echo")
                .setHandlerFunction("handle");
        List<String> items = IntStream.range(0, ITEM_COUNT).mapToObj(Integer::toString).collect(toList());
        Pipeline p = Pipeline.create();
        BatchStage<String> stage = p.readFrom(TestSources.items(items));

        // When
        BatchStage<String> mapped = stage.apply(mapUsingPythonBatch(cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                 "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
    }

    @Test
    @Category(NightlyTest.class)
    public void batchStage_mapUsingPython_onAllMembers() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setHandlerFile(baseDir + "/echo.py")
                .setHandlerFunction("handle");
        List<String> items = IntStream.range(0, ITEM_COUNT).mapToObj(Integer::toString).collect(toList());
        Pipeline p = Pipeline.create();
        BatchStage<String> stage = p.readFrom(TestSources.items(items));

        // When
        BatchStage<String> mapped = stage.apply(mapUsingPythonBatch(x -> x, cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPython_withBaseDir() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("echo")
                .setHandlerFunction("handle");
        List<String> items = IntStream.range(0, ITEM_COUNT).mapToObj(Integer::toString).collect(toList());
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0);

        // When
        StreamStage<String> mapped = stage.apply(mapUsingPython(cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                 "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPython_withHandlerFile() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setHandlerFile(baseDir + "/echo.py")
                .setHandlerFunction("handle");
        List<String> items = IntStream.range(0, ITEM_COUNT).mapToObj(Integer::toString).collect(toList());
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0);

        // When
        StreamStage<String> mapped = stage.apply(mapUsingPython(cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                 "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPython_onAllMembers() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setHandlerFile(baseDir + "/echo.py")
                .setHandlerFunction("handle");
        List<String> items = IntStream.range(0, ITEM_COUNT).mapToObj(Integer::toString).collect(toList());
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items(items))
                                     .addTimestamps(x -> 0, 0);

        // When
        StreamStage<String> mapped = stage.apply(mapUsingPython(x -> x, cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                 "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPython_withInitScript() throws Exception {
        // Given
        String baseDirStr = baseDir.toString();
        String outcomeFilename = "init_outcome.txt";
        installFileToBaseDir(
                String.format(
                        "echo 'Init script running'%n" +
                        "echo 'init.sh' executed > %s/%s%n", baseDirStr, outcomeFilename),
                "init.sh");
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDirStr)
                .setHandlerModule("echo")
                .setHandlerFunction("handle");
        List<String> items = singletonList("item-0");
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0);

        // When
        StreamStage<String> mapped = stage.apply(mapUsingPython(cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
        assertTrue("Init script didn't run", new File(baseDir, outcomeFilename).isFile());
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPython_withCleanupScript() throws Exception {
        // Given
        String baseDirStr = baseDir.toString();
        String outcomeFilename = "cleanup_outcome.txt";
        installFileToBaseDir(
                String.format(
                        "echo 'Cleanup script running'%n" +
                        "echo 'cleanup.sh' executed > %s/%s%n", baseDirStr, outcomeFilename),
                "cleanup.sh");
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDirStr)
                .setHandlerModule("echo")
                .setHandlerFunction("handle");
        List<String> items = singletonList("item-0");
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items(items)).addTimestamps(x -> 0, 0);

        // When
        StreamStage<String> mapped = stage.apply(mapUsingPython(cfg)).setLocalParallelism(2);

        // Then
        mapped.writeTo(AssertionSinks.assertAnyOrder(
                "Python didn't map the items correctly", items.stream().map(i -> "echo-" + i).collect(toList())
        ));
        instance().getJet().newJob(p).join();
        assertTrue("Cleanup script didn't run", new File(baseDir, outcomeFilename).isFile());
    }

    @Test
    @Category(NightlyTest.class)
    public void batchStage_mapUsingPythonFailure() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("echo")
                .setHandlerFunction("notExistsFunction");
        Pipeline p = Pipeline.create();
        BatchStage<String> stage = p.readFrom(TestSources.items("1"));

        // When
        stage.apply(mapUsingPythonBatch(cfg)).setLocalParallelism(2)
                .writeTo(Sinks.logger());

        // Then
        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException ex) {
            // expected
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPythonFailure() {
        // Given
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("echo")
                .setHandlerFunction("notExistsFunction");
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items("1")).addTimestamps(x -> 0, 0);

        // When
        stage.apply(mapUsingPython(cfg)).setLocalParallelism(2)
                .writeTo(Sinks.logger());

        // Then
        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException ex) {
            // expected
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void batchStage_mapUsingPythonException() throws IOException {
        // Given
        installFileToBaseDir(FAILING_FUNCTION, "failing.py");
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("failing")
                .setHandlerFunction("handle");
        Pipeline p = Pipeline.create();
        BatchStage<String> stage = p.readFrom(TestSources.items("1"));

        // When
        stage.apply(mapUsingPythonBatch(cfg)).setLocalParallelism(2)
                .writeTo(Sinks.logger());

        // Then
        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException ex) {
            // expected
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void streamStage_mapUsingPythonException() throws IOException {
        // Given
        installFileToBaseDir(FAILING_FUNCTION, "failing.py");
        PythonServiceConfig cfg = new PythonServiceConfig()
                .setBaseDir(baseDir.toString())
                .setHandlerModule("failing")
                .setHandlerFunction("handle");
        Pipeline p = Pipeline.create();
        StreamStage<String> stage = p.readFrom(TestSources.items("1")).addTimestamps(x -> 0, 0);

        // When
        stage.apply(mapUsingPython(cfg)).setLocalParallelism(2)
                .writeTo(Sinks.logger());

        // Then
        try {
            instance().getJet().newJob(p).join();
            fail();
        } catch (CompletionException ex) {
            // expected
        }
    }

    private void installFileToBaseDir(String contents, String filename) throws IOException {
        try (InputStream in = new ByteArrayInputStream(contents.getBytes(UTF_8))) {
            Files.copy(in, new File(baseDir, filename).toPath());
        }
    }
}
