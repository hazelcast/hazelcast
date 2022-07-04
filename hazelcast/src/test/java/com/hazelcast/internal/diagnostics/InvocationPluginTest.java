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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationPluginTest extends AbstractDiagnosticsPluginTest {

    private InvocationSamplePlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(InvocationSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "1")
                .setProperty(InvocationSamplePlugin.SLOW_THRESHOLD_SECONDS.getName(), "5");

        hz = createHazelcastInstance(config);

        plugin = new InvocationSamplePlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        spawn(() -> {
            hz.getMap("foo").executeOnKey(randomString(), new SlowEntryProcessor());
        });

        assertTrueEventually(() -> {
            plugin.run(logWriter);

            assertContains(EntryOperation.class.getName());
        });
    }

    static class SlowEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException ignored) {
            }
            return null;
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }
}
