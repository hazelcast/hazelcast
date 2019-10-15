/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.LinkedList;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RegisterMetricTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test(expected = NullPointerException.class)
    public void whenNamePrefixNull() {
        metricsRegistry.registerStaticMetrics(new SomeField(), null);
    }

    @Test(expected = NullPointerException.class)
    public void whenObjectNull() {
        metricsRegistry.registerStaticMetrics(null, "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUnrecognizedField() {
        metricsRegistry.registerStaticMetrics(new SomeUnrecognizedField(), "bar");
    }

    @Test
    public void whenNoGauges_thenIgnore() {
        metricsRegistry.registerStaticMetrics(new LinkedList(), "bar");

        for (String name : metricsRegistry.getNames()) {
            assertFalse(name.startsWith("bar"));
        }
    }

    public class SomeField {
        @Probe
        long field;
    }

    public class SomeUnrecognizedField {
        @Probe
        OutputStream field;
    }

    public class MultiFieldAndMethod {
        @Probe
        long field1;
        @Probe
        long field2;

        @Probe
        int method1() {
            return 1;
        }

        @Probe
        int method2() {
            return 2;
        }
    }
}
