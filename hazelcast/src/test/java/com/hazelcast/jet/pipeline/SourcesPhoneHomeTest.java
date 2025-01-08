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

package com.hazelcast.jet.pipeline;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.jet.pipeline.JetPhoneHomeTestUtil.assertConnectorPhoneHomeCollected;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SourcesPhoneHomeTest extends JetTestSupport {
    private HazelcastInstance[] instances;
    private File directory;

    @Before
    public void setUp() throws IOException {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.setClusterName(randomName());
        instances = createHazelcastInstances(config, 2);
        directory = createTempDirectory();
        File fooFile = new File(directory, "foo.txt");
        fooFile.createNewFile();
    }

    @After
    public void cleanup() {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                assertTrue(file.delete());
            }
        }
        assertTrue(directory.delete());
    }


    @Test
    public void fileWatcher() {
        StreamSource<String> source = Sources.fileWatcher(directory.getPath());

        Pipeline p = Pipeline.create();
        p.readFrom(source).withoutTimestamps().writeTo(Sinks.noop());

        List<Supplier<Pipeline>> pipelines = new ArrayList<>();
        pipelines.add(() -> p);

        assertConnectorPhoneHomeCollected(pipelines, ConnectorNames.FILE_WATCHER, null, false, instances);
    }

    @Test
    public void fileBuilder_build() {
        BatchSource<String> source = Sources.filesBuilder(directory.getPath()).build();

        Pipeline p = Pipeline.create();
        p.readFrom(source).writeTo(Sinks.noop());

        List<Supplier<Pipeline>> pipelines = new ArrayList<>();
        pipelines.add(() -> p);

        assertConnectorPhoneHomeCollected(pipelines, ConnectorNames.FILES, null, true, instances);
    }

    @Test
    public void fileBuilder_buildWatcher() {
        StreamSource<String> source2 = Sources.filesBuilder(directory.getPath()).buildWatcher();

        Pipeline p2 = Pipeline.create();
        p2.readFrom(source2).withoutTimestamps().writeTo(Sinks.noop());

        List<Supplier<Pipeline>> pipelines2 = new ArrayList<>();
        pipelines2.add(() -> p2);

        assertConnectorPhoneHomeCollected(pipelines2, ConnectorNames.FILE_WATCHER, null, false, instances);
    }
}
