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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.diagnostics.DiagnosticsPlugin.DISABLED;
import static com.hazelcast.internal.diagnostics.SystemLogPlugin.ENABLED;
import static com.hazelcast.internal.diagnostics.SystemLogPlugin.LOG_PARTITIONS;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("WeakerAccess")
public class SystemLogPluginTest extends AbstractDiagnosticsPluginTest {

    protected Config config;
    protected TestHazelcastInstanceFactory hzFactory;
    protected HazelcastInstance hz;
    protected SystemLogPlugin plugin;

    @Before
    public void setup() {
        config = new Config();
        config.setProperty(LOG_PARTITIONS.getName(), "true");

        hzFactory = createHazelcastInstanceFactory(2);
        hz = hzFactory.newHazelcastInstance(config);
        plugin = new SystemLogPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodSeconds() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testGetPeriodSeconds_whenPluginIsDisabled_thenReturnDisabled() {
        config.setProperty(ENABLED.getName(), "false");
        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);

        plugin = new SystemLogPlugin(getNodeEngineImpl(instance));
        plugin.onStart();

        assertEquals(DISABLED, plugin.getPeriodMillis());
    }

    @Test
    public void testLifecycle() {
        hz.shutdown();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);

                assertContains("Lifecycle[" + LINE_SEPARATOR + "                          SHUTTING_DOWN]");
            }
        });
    }

    @Test
    public void testMembership() {
        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);
                assertContains("MemberAdded[");
            }
        });

        instance.shutdown();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);
                assertContains("MemberRemoved[");
            }
        });
    }

    @Test
    public void testMigration() {
        warmUpPartitions(hz);

        HazelcastInstance instance = hzFactory.newHazelcastInstance(config);
        warmUpPartitions(instance);

        waitAllForSafeState(hz, instance);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);
                assertContains("MigrationCompleted");
            }
        });
    }
}
