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

import static com.hazelcast.internal.metrics.ProbeUtils.probeAllInstances;
import static org.junit.Assert.assertArrayEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.AbstractMetricsTest;
import com.hazelcast.internal.metrics.BeforeCollectionCycle;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.CollectionCycle.Tags;
import com.hazelcast.internal.metrics.ProbingContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * While there are specific tests for the basic {@link Probe} annotated field
 * and method mapping this class focuses on the more advanced scenarios.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MetricsRegistryTest extends AbstractMetricsTest implements MetricsSource {

    @Before
    public void setUp() {
        registry.register(this);
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
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
        cycle.collectAll("", new AdjoinSource());
    }

    private static final class LevelBean implements ProbingContext {

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
        public void tag(Tags context) {
            if (name.equals("special")) {
                context.tag(TAG_TARGET, name);
            } else if (!name.isEmpty()) {
                context.tag(TAG_INSTANCE, name);
            }
        }
    }

    /**
     * Illustrates a nested bean with a prefix in the type level annotation
     */
    @Probe(name = "path")
    private static final class NestedA {

        @Probe
        long val;

        @Probe
        NestedA sub;

        public NestedA(long val, NestedA sub) {
            this.val = val;
            this.sub = sub;
        }
    }

    /**
     * Illustrates a nested bean with no prefix in the type level annotation
     */
    @Probe
    private static final class NestedB {
        @Probe
        long x;
        @Probe
        NestedB sub;

        public NestedB(long x, NestedB sub) {
            this.x = x;
            this.sub = sub;
        }
    }

    private static final class NestedSource implements MetricsSource {

        @Probe
        long y;

        @Probe
        NestedSource sub;

        @Probe
        long z = 42;

        NestedSource(long y, NestedSource sub) {
            this.y = y;
            this.sub = sub;
        }

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.openContext(); // just to test relative nesting
            cycle.probe(this);
            cycle.probe("my", this);
        }
    }

    private static final class AdjoinSource implements MetricsSource {

        @Probe
        long age = 13;

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.openContext().tag("foo", "bar").adjoin("tender");
            cycle.probe(this);
        }

    }

    @Test
    public void onlyProbesOfEnabledLevelsAreCollected() {
        setLevel(ProbeLevel.MANDATORY);
        assertCollectedCount(8);
        assertMetric("mandatory", 1);
        setLevel(ProbeLevel.INFO);
        assertCollectedCount(34); // 2x8 + 1 + 4 + 12 + 1
        assertMetric("mandatory", 1);
        assertMetric("info", 2);
        setLevel(ProbeLevel.DEBUG);
        assertCollectedCount(42); // 3x8 + 1 + 4 + 12 + 1
        assertMetric("mandatory", 1);
        assertMetric("info", 2);
        assertMetric("debug", 3);
    }

    private void assertMetric(String name, long value) {
        String i = TAG_INSTANCE;
        String t = TAG_TYPE;
        assertCollected(i + "=a foo." + name, value);
        assertCollected(i + "=b bar." + name, value);
        assertCollected("baz." + name, value);
        assertCollected("target=special " + name, value);
        assertCollected(t + "=map " + i + "=a " + name, value);
        assertCollected(t + "=map " + i + "=b " + name, value);
        assertCollected(t + "=map " + i + "=z " + name, value);
        assertCollected(t + "=map " + i + "=s target=special " + name, value);
    }

    @Probe
    private int updates = 0;

    @Probe(name = "a")
    private NestedA nestedA = new NestedA(1, new NestedA(2, null));

    @Probe(name = "b")
    private NestedB nestedB = new NestedB(1, new NestedB(2, null));

    @Probe(name = "c")
    private NestedSource nestedSource = new NestedSource(1, new NestedSource(2, null));

    @BeforeCollectionCycle(value = 500, unit = TimeUnit.MILLISECONDS)
    private void update() {
        updates++;
    }

    @Test
    public void reprobingOccursInSpecifiedCycleTime() {
        assertCollected("updates", 1);
        assertCollected("updates", 1);
        sleepAtLeastMillis(501L);
        assertCollected("updates", 2);
        assertCollected("updates", 2);
    }

    @Test
    public void nestedProbing() {
        assertCollected("a.path.val", 1L);
        assertCollected("a.path.sub.path.val", 2L);
        assertCollected("b.x", 1L);
        assertCollected("b.sub.x", 2L);
    }

    @Test
    public void nestedSourceCollection() {
        assertCollected("c.y", 1L);
        assertCollected("c.z", 42L);
        assertCollected("c.sub.y", 2L);
        assertCollected("c.sub.z", 42L);
        assertCollected("c.sub.my.y", 2L);
        assertCollected("c.sub.my.z", 42L);
        assertCollected("c.my.y", 1L);
        assertCollected("c.my.z", 42L);
        assertCollected("c.my.sub.y", 2L);
        assertCollected("c.my.sub.z", 42L);
        assertCollected("c.my.sub.my.y", 2L);
        assertCollected("c.my.sub.my.z", 42L);
    }

    @Test
    public void adjoinTagCollection() {
        assertCollected("foo=bartender age", 13L);
    }

    @Test
    public void sort() {
        String[] names = { "a", "c", "b" };
        Integer[] values = { 1, 3, 2 };
        MetricsRegistryImpl.sort(names, values);
        assertArrayEquals(new String[] { "a", "b", "c" }, names);
        assertArrayEquals(new Integer[] { 1, 2, 3 }, values);
    }
}
