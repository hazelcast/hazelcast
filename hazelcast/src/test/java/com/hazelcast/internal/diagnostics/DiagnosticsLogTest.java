/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiagnosticsLogTest extends HazelcastTestSupport {

    private DiagnosticsLogFile diagnosticsLogFile;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(Diagnostics.MAX_ROLLED_FILE_SIZE_MB.getName(), "0.2")
                .setProperty(Diagnostics.MAX_ROLLED_FILE_COUNT.getName(), "3")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");

        HazelcastInstance hz = createHazelcastInstance(config);

        Diagnostics diagnostics = getDiagnostics(hz);

        diagnosticsLogFile = diagnostics.diagnosticsLogFile;
        metricsRegistry = getMetricsRegistry(hz);
    }

    @AfterClass
    public static void afterClass() {
        String userDir = System.getProperty("user.dir");

        File[] files = new File(userDir).listFiles();
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith("diagnostics-") && name.endsWith(".log")) {
                    deleteQuietly(file);
                }
            }
        }
    }

    @Test
    public void testDisabledByDefault() {
        HazelcastProperties hazelcastProperties = new HazelcastProperties(new Config());
        assertFalse(hazelcastProperties.getBoolean(Diagnostics.ENABLED));
    }

    @Test
    public void testRollover() {
        String id = generateRandomString(10000);

        final List<File> files = new LinkedList<File>();

        LongProbeFunction f = new LongProbeFunction() {
            @Override
            public long get(Object source) {
                return 0;
            }
        };

        for (int k = 0; k < 10; k++) {
            metricsRegistry.register(this, id + k, ProbeLevel.MANDATORY, f);
        }

        // we run for some time to make sure we get enough rollovers
        while (files.size() < 3) {
            final File file = diagnosticsLogFile.file;
            if (file != null) {
                if (!files.contains(file)) {
                    files.add(file);
                }

                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() {
                        assertExist(file);
                    }
                });
            }

            sleepMillis(100);
        }

        // eventually all these files should be gone
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (File file : files) {
                    assertNotExist(file);
                }
            }
        });
    }

    private static void assertNotExist(File file) {
        assertFalse("file:" + file + " should not exist", file.exists());
    }

    private static void assertExist(File file) {
        assertTrue("file:" + file + " should exist", file.exists());
    }

    @Test
    public void test() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                String content = loadLogfile();
                assertNotNull(content);

                assertContains(content, "SystemProperties[");
                assertContains(content, "BuildInfo[");
                assertContains(content, "ConfigProperties[");
                assertContains(content, "Metric[");
            }
        });
    }

    private String loadLogfile() {
        File file = diagnosticsLogFile.file;
        if (file == null || !file.exists()) {
            return null;
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            try {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    sb.append(LINE_SEPARATOR);
                    line = br.readLine();
                }
                return sb.toString();
            } finally {
                br.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Diagnostics getDiagnostics(HazelcastInstance hazelcastInstance) {
        Node node = getNode(hazelcastInstance);
        NodeEngineImpl nodeEngine = node.nodeEngine;

        try {
            Field field = NodeEngineImpl.class.getDeclaredField("diagnostics");
            field.setAccessible(true);
            return (Diagnostics) field.get(nodeEngine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
