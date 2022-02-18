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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NetworkingImbalancePluginTest extends AbstractDiagnosticsPluginTest {

    private NetworkingImbalancePlugin plugin;
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(NetworkingImbalancePlugin.PERIOD_SECONDS.getName(), "1");

        // we need to start a real Hazelcast instance here, since the mocked network doesn't have a TcpIpConnectionManager
        hz = Hazelcast.newHazelcastInstance(config);

        plugin = new NetworkingImbalancePlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @After
    public void tearDown() {
        hz.shutdown();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(1000, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        spawn(new Runnable() {
            @Override
            public void run() {
                hz.getMap("foo").put("key", "value");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);

                assertContains("Networking");
                assertContains("InputThreads");
                assertContains("OutputThreads");
            }
        });
    }

    @Test
    public void noNaNPercentagesForZeroAmounts() {
        spawn(new Runnable() {
            @Override
            public void run() {
                hz.getMap("foo").put("key", "value");
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                plugin.run(logWriter);

                assertNotContains("NaN");
            }
        });
    }
}
