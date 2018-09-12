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

package com.hazelcast.internal.probing;

import static com.hazelcast.internal.probing.ProbeRegistryImpl.toLong;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.util.Collections.singletonMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeAnnotatedFieldsTest extends ProbingTest implements ProbeSource {

    @Before
    public void setUp() {
        registry.register(this);
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.probe("foo", new ProbeAnnotatedFields());
        cycle.probe("foo", new Subclass());
    }

    private static final class ProbeAnnotatedFields {
        @Probe(name = "myfield")
        private long field = 10L;

        @Probe
        private int intField = 10;

        @Probe
        private long longField = 10L;

        @Probe
        private double doubleField = 10d;

        @Probe
        private ConcurrentHashMap<String, String> mapField = new ConcurrentHashMap<String, String>(
                singletonMap("x", "y"));

        @Probe
        private Counter counterField = newSwCounter(10L);

        @Probe
        static AtomicInteger staticfield = new AtomicInteger(10);
    }

    @Test
    public void customName() {
        assertProbed("foo.myfield", 10L);
    }

    @Test
    public void primitiveInteger() {
        assertProbed("foo.intField", 10L);
    }

    @Test
    public void primitiveLong() {
        assertProbed("foo.longField", 10L);
    }

    @Test
    public void primitiveDouble() {
        assertProbed("foo.doubleField", toLong(10d));
    }

    @Test
    public void concurrentHashMap() {
        assertProbed("foo.mapField", 1L);
    }

    @Test
    public void counterFields() {
        assertProbed("foo.counterField", 10L);
    }

    @Test
    public void staticField() {
        assertProbed("foo.staticfield", 10L);
    }

    @Test
    public void superclassRegistration() {
        assertProbed("foo.inheritedField", 10L);
    }

    private static class SuperClass {
        @Probe
        int inheritedField = 10;
    }

    private static class Subclass extends SuperClass {

    }
}
