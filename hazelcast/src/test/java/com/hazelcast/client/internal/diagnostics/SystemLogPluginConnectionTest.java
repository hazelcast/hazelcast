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

package com.hazelcast.client.internal.diagnostics;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.AbstractDiagnosticsPluginTest;
import com.hazelcast.internal.diagnostics.SystemLogPlugin;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.diagnostics.SystemLogPlugin.LOG_PARTITIONS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SystemLogPluginConnectionTest extends AbstractDiagnosticsPluginTest {

    private TestHazelcastFactory hzFactory;
    private SystemLogPlugin plugin;

    @Before
    public void setUp() {
        Config config = new Config();
        config.setProperty(LOG_PARTITIONS.getName(), "true");

        hzFactory = new TestHazelcastFactory();
        HazelcastInstance hz = hzFactory.newHazelcastInstance(config);
        plugin = new SystemLogPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @After
    public void tearDown() {
        hzFactory.terminateAll();
    }

    @Test
    public void testConnection() {
        HazelcastInstance instance = hzFactory.newHazelcastClient();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);
                assertContains("ConnectionAdded");
            }
        });

        instance.shutdown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                plugin.run(logWriter);
                assertContains("ConnectionRemoved");
            }
        });
    }
}
