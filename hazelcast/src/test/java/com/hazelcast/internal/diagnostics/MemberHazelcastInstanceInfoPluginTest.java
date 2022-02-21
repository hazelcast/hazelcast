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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemberHazelcastInstanceInfoPluginTest extends AbstractDiagnosticsPluginTest {

    private MemberHazelcastInstanceInfoPlugin plugin;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new MemberHazelcastInstanceInfoPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(SECONDS.toMillis(60), plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        plugin.run(logWriter);

        assertContains("HazelcastInstance[");
        assertContains("isRunning=true");
        assertContains("Members[");
    }
}
