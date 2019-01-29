/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RegisterMetricTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test(expected = NullPointerException.class)
    public void whenNamePrefixNull() {
        metricsRegistry.scanAndRegister(new SomeField(), null);
    }

    @Test(expected = NullPointerException.class)
    public void whenObjectNull() {
        metricsRegistry.scanAndRegister(null, "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUnrecognizedField() {
        metricsRegistry.scanAndRegister(new SomeUnrecognizedField(), "bar");
    }

    @Test
    public void whenNoGauges_thenIgnore() {
        metricsRegistry.scanAndRegister(new LinkedList(), "bar");

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

    @Test
    public void deregister_whenNotRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));
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

    @Test
    public void deregister_whenRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.scanAndRegister(multiFieldAndMethod, "foo");

        LongGauge field1 = metricsRegistry.newLongGauge("foo.field1");
        LongGauge field2 = metricsRegistry.newLongGauge("foo.field2");
        LongGauge method1 = metricsRegistry.newLongGauge("foo.method1");
        LongGauge method2 = metricsRegistry.newLongGauge("foo.method2");

        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));

        // make sure that the metric input has been disconnected
        assertEquals(0, field1.read());
        assertEquals(0, field2.read());
        assertEquals(0, method1.read());
        assertEquals(0, method2.read());
    }

    @Test
    public void deregister_whenAlreadyDeregistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.scanAndRegister(multiFieldAndMethod, "foo");
        metricsRegistry.deregister(multiFieldAndMethod);
        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));
    }
}
