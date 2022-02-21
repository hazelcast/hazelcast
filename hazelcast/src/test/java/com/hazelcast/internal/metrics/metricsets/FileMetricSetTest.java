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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class FileMetricSetTest extends HazelcastTestSupport {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        FileMetricSet.register(metricsRegistry);
    }

    @After
    public void tearDown() {
        metricsRegistry.shutdown();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(FileMetricSet.class);
    }

    @Test
    public void freeSpace() {
        File file = new File(System.getProperty("user.home"));

        LongGauge freeSpaceGauge =
                metricsRegistry.newLongGauge("file.partition[user.home].freeSpace");

        assertAlmostEquals(file.getFreeSpace(), freeSpaceGauge.read());
    }

    @Test
    public void totalSpace() {
        File file = new File(System.getProperty("user.home"));

        LongGauge totalSpaceGauge =
                metricsRegistry.newLongGauge("file.partition[user.home].totalSpace");

        assertAlmostEquals(file.getTotalSpace(), totalSpaceGauge.read());
    }

    @Test
    public void usableSpace() {
        File file = new File(System.getProperty("user.home"));

        LongGauge usableSpaceGauge =
                metricsRegistry.newLongGauge("file.partition[user.home].usableSpace");

        assertAlmostEquals(file.getUsableSpace(), usableSpaceGauge.read());
    }

    public static void assertAlmostEquals(long expected, long found) {
        assertTrue(found >= 0.9 * expected);
        assertTrue(found <= 1.1 * expected);
    }
}
