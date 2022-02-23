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

import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbeImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class StoreLatencyPluginTest extends AbstractDiagnosticsPluginTest {

    private StoreLatencyPlugin plugin;

    @Before
    public void setup() {
        Properties p = new Properties();
        p.put(StoreLatencyPlugin.PERIOD_SECONDS, "1");
        HazelcastProperties properties = new HazelcastProperties(p);
        plugin = new StoreLatencyPlugin(Logger.getLogger(StoreLatencyPlugin.class), properties);
    }

    @Test
    public void getProbe() {
        LatencyProbe probe = plugin.newProbe("foo", "queue", "somemethod");
        assertNotNull(probe);
    }

    @Test
    public void getProbe_whenSameProbeRequestedMoreThanOnce() {
        LatencyProbe probe1 = plugin.newProbe("foo", "queue", "somemethod");
        LatencyProbe probe2 = plugin.newProbe("foo", "queue", "somemethod");
        assertSame(probe1, probe2);
    }

    @Test
    public void testMaxMicros() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(1000));
        probe.recordValue(MICROSECONDS.toNanos(4));

        assertEquals(1000, probe.distribution.maxMicros());
    }

    @Test
    public void testCount() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(10));

        assertEquals(3, probe.distribution.count());
    }

    @Test
    public void testTotalMicros() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(10));
        probe.recordValue(MICROSECONDS.toNanos(20));
        probe.recordValue(MICROSECONDS.toNanos(30));

        assertEquals(60, probe.distribution.totalMicros());
    }

    @Test
    public void render() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(100));
        probe.recordValue(MICROSECONDS.toNanos(200));
        probe.recordValue(MICROSECONDS.toNanos(200));
        probe.recordValue(MICROSECONDS.toNanos(300));

        plugin.run(logWriter);

        assertContains("foo");
        assertContains("queue");
        assertContains("somemethod");
        assertContains("count=4");
        assertContains("totalTime(us)=800");
        assertContains("avg(us)=200");
        assertContains("max(us)=300");
        assertContains("64..127us=1");
        assertContains("128..255us=2");
        assertContains("256..511us=1");
    }

    @Test
    public void max_latency_goes_right_distribution_bucket() {
        LatencyProbeImpl probe = (LatencyProbeImpl) plugin.newProbe("foo", "queue", "somemethod");
        probe.recordValue(MICROSECONDS.toNanos(4));

        plugin.run(logWriter);

        assertContains("4..7us=1");
    }
}
