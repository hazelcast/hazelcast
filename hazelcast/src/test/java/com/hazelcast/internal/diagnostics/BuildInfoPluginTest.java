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
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BuildInfoPluginTest extends AbstractDiagnosticsPluginTest {

    private BuildInfoPlugin plugin;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new BuildInfoPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    @Test
    public void test() throws IOException {
        plugin.run(logWriter);

        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();

        assertContains("BuildNumber=" + buildInfo.getBuildNumber());
        assertContains("Build=" + buildInfo.getBuild());
        assertContains("Revision=" + buildInfo.getRevision());
        assertContains("Version=" + buildInfo.getVersion());
        assertContains("SerialVersion=" + buildInfo.getSerializationVersion());
        assertContains("Enterprise=" + buildInfo.isEnterprise());
    }
}
