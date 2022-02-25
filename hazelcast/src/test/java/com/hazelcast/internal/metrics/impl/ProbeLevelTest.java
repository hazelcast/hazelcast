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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeLevelTest extends HazelcastTestSupport {

    private ILogger logger;

    @Before
    public void setup() {
        logger = Logger.getLogger(MetricsRegistryImpl.class);
    }

    @Test
    public void test() {
        assertProbeExist(MANDATORY, MANDATORY);
        assertNotProbeExist(INFO, MANDATORY);
        assertNotProbeExist(DEBUG, MANDATORY);

        assertProbeExist(MANDATORY, INFO);
        assertProbeExist(INFO, INFO);
        assertNotProbeExist(DEBUG, INFO);

        assertProbeExist(MANDATORY, DEBUG);
        assertProbeExist(INFO, DEBUG);
        assertProbeExist(DEBUG, DEBUG);
    }

    public void assertProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.registerStaticProbe(this, "foo", probeLevel, (LongProbeFunction<ProbeLevelTest>) source -> 10);

        assertContains(metricsRegistry.getNames(), "[metric=foo]");
        assertEquals(10, metricsRegistry.newLongGauge("foo").read());
    }

    public void assertNotProbeExist(ProbeLevel probeLevel, ProbeLevel minimumLevel) {
        MetricsRegistryImpl metricsRegistry = new MetricsRegistryImpl(logger, minimumLevel);

        metricsRegistry.registerStaticProbe(this, "foo", probeLevel, (LongProbeFunction<ProbeLevelTest>) source -> 10);

        assertFalse(metricsRegistry.getNames().contains("[metric=foo]"));
        assertEquals(0, metricsRegistry.newLongGauge("foo").read());
    }
}
