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

package com.hazelcast.internal.probing.impl;

import static com.hazelcast.internal.probing.Probing.probeAllInstances;
import static org.junit.Assert.assertArrayEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.AbstractProbeTest;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.internal.probing.ReprobeCycle;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle.Tagging;
import com.hazelcast.internal.probing.ProbingCycle.Tags;
import com.hazelcast.internal.probing.impl.ProbeRegistryImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * While there are specific tests for the basic {@link Probe} annotated field
 * and method mapping this class focuses on the more advanced scenarios.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeRegistryTest extends AbstractProbeTest implements ProbeSource {

    @Before
    public void setUp() {
        registry.register(this);
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        LevelBean a = new LevelBean("a");
        LevelBean b = new LevelBean("b");
        LevelBean c = new LevelBean("");
        LevelBean d = new LevelBean("special");
        // context should be clean - no openContext required
        cycle.probe("foo", a); // should be context neutral
        // context should still be clean, again no openContext
        cycle.probe(d); // should be context neutral
        // context should still be clean, again no openContext
        cycle.probe("bar", b);
        // and again
        cycle.probe("baz", c);
        Map<String, LevelBean> map = new HashMap<String, LevelBean>();
        map.put("x", a);
        map.put("y", b);
        map.put("z", c);
        map.put("s", d);
        probeAllInstances(cycle, "map", map);
        cycle.openContext(); // needed to reset context
        cycle.probe(this);
    }

    private static final class LevelBean implements Tagging {

        private final String name;

        public LevelBean(String name) {
            this.name = name;
        }

        @Probe(level = ProbeLevel.MANDATORY)
        long mandatory = 1;

        @Probe(level = ProbeLevel.INFO)
        long info = 2;

        @Probe(level = ProbeLevel.DEBUG)
        long debug = 3;

        @Override
        public void tagIn(Tags context) {
            if (name.equals("special")) {
                context.tag(TAG_TARGET, name);
            } else if (!name.isEmpty()) {
                context.tag(TAG_INSTANCE, name);
            }
        }
    }

    @Test
    public void onlyProbesOfEnabledLevelsAreRendered() {
        setLevel(ProbeLevel.MANDATORY);
        assertProbeCount(8);
        assertProbes("mandatory", 1);
        setLevel(ProbeLevel.INFO);
        assertProbeCount(17); // 2x8 + 1
        assertProbes("mandatory", 1);
        assertProbes("info", 2);
        setLevel(ProbeLevel.DEBUG);
        assertProbeCount(25); // 3x8 + 1
        assertProbes("mandatory", 1);
        assertProbes("info", 2);
        assertProbes("debug", 3);
    }

    private void assertProbes(String name, long value) {
        String i = TAG_INSTANCE;
        String t = TAG_TYPE;
        assertProbed(i + "=a foo." + name, value);
        assertProbed(i + "=b bar." + name, value);
        assertProbed("baz." + name, value);
        assertProbed("target=special " + name, value);
        assertProbed(t + "=map " + i + "=a " + name, value);
        assertProbed(t + "=map " + i + "=b " + name, value);
        assertProbed(t + "=map " + i + "=z " + name, value);
        assertProbed(t + "=map " + i + "=s target=special " + name, value);
    }

    @Probe
    private int updates = 0;

    @ReprobeCycle(value = 500, unit = TimeUnit.MILLISECONDS)
    private void update() {
        updates++;
    }

    @Test
    public void reprobingOccursInSpecifiedCycleTime() {
        assertProbed("updates", 1);
        assertProbed("updates", 1);
        sleepAtLeastMillis(501L);
        assertProbed("updates", 2);
        assertProbed("updates", 2);
    }

    @Test
    public void sort() {
        String[] names = { "a", "c", "b" };
        Integer[] values = { 1, 3, 2 };
        ProbeRegistryImpl.sort(names, values);
        assertArrayEquals(new String[] { "a", "b", "c" }, names);
        assertArrayEquals(new Integer[] { 1, 2, 3 }, values);
    }
}
