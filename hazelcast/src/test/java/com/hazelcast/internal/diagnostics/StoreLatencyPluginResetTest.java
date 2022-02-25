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

import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StoreLatencyPluginResetTest extends AbstractDiagnosticsPluginTest {

    @Test
    public void test() {
        Properties props = new Properties();
        props.put(StoreLatencyPlugin.PERIOD_SECONDS.getName(), "1");
        props.put(StoreLatencyPlugin.RESET_PERIOD_SECONDS.getName(), "2");

        HazelcastProperties properties = new HazelcastProperties(props);
        StoreLatencyPlugin plugin = new StoreLatencyPlugin(Logger.getLogger(StoreLatencyPlugin.class), properties);

        StoreLatencyPlugin.LatencyProbe probe = plugin.newProbe("foo", "queue", "somemethod");

        probe.recordValue(MICROSECONDS.toNanos(1));
        probe.recordValue(MICROSECONDS.toNanos(2));
        probe.recordValue(MICROSECONDS.toNanos(5));

        // run for the first time
        plugin.run(logWriter);
        assertContains("max(us)=5");

        // reset the logWriter so we don't get previous run content
        reset();
        // run for the second time;
        plugin.run(logWriter);
        // now it should still contain the old statistics
        assertContains("max(us)=5");

        // reset the logWriter so we don't get previous run content
        reset();
        // run for the third time; now the stats should be gone
        plugin.run(logWriter);
        // now it should not contain the old statistics
        assertNotContains("max(us)=5");
    }
}
