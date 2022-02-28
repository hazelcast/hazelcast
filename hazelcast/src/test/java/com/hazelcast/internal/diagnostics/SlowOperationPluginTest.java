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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.operation.EntryOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SlowOperationPluginTest extends AbstractDiagnosticsPluginTest {

    private SlowOperationPlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(SLOW_OPERATION_DETECTOR_ENABLED.getName(), "true")
                .setProperty(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS.getName(), "1000")
                .setProperty(SlowOperationPlugin.PERIOD_SECONDS.getName(), "1");

        hz = createHazelcastInstance(config);

        plugin = new SlowOperationPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        spawn(() -> {
            hz.getMap("foo").executeOnKey("bar", new SlowEntryProcessor());
        });

        assertTrueEventually(() -> {
            plugin.run(logWriter);
            assertContains(EntryOperation.class.getName());
            assertContains("stackTrace");
            assertContains("invocations=1");
            assertContains("startedAt=");
            assertContains("duration(ms)=");
            assertContains("operationDetails=");
        });
    }

    static class SlowEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }
}
