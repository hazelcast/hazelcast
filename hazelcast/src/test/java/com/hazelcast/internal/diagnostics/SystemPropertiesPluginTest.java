/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Properties;
import java.util.stream.Stream;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SystemPropertiesPluginTest extends AbstractDiagnosticsPluginTest {

    private static final String FAKE_PROPERTY = "hazelcast.fake.property";
    private static final String FAKE_PROPERTY_VALUE = "foobar";

    private SystemPropertiesPlugin plugin;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new SystemPropertiesPlugin(getNodeEngineImpl(hz));
        plugin.onStart();
        System.setProperty(FAKE_PROPERTY, "foobar");
    }

    @After
    public void tearDown() {
        System.clearProperty(FAKE_PROPERTY);
    }

    @Test
    public void testGetPeriodMillis() {
        assertEquals(DiagnosticsPlugin.STATIC, plugin.getPeriodMillis());
    }

    @Test
    public void testRun() {
        plugin.run(logWriter);

        final Properties systemProperties = System.getProperties();

        // we check a few of the regular ones
        // we want to make sure the hazelcast system properties are added
        Stream.of("java.class.version", "java.class.path", FAKE_PROPERTY, "java.vm.args")
                .forEach(key -> assertContains(key + "=" + systemProperties.get(key)));

        // we don't want to have awt
        assertNotContains("java.awt");
    }
}
