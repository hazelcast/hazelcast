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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.AbstractMetricsTest;
import com.hazelcast.internal.metrics.BeforeCollectionCycle;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeSource;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.CollectionCycle.Tags;
import com.hazelcast.internal.metrics.ObjectMetricsContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * While there are specific tests for the basic {@link Probe} annotated field
 * and method mapping this class focuses on the more advanced scenarios.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MetricsRegistryTest extends AbstractMetricsTest {

    private static class LevelDependentMetrics implements MetricsSource {

        @Override
        public void collectAll(CollectionCycle cycle) {
            LevelBean a = new LevelBean("a");
            LevelBean b = new LevelBean("b");
            LevelBean c = new LevelBean("");
            LevelBean d = new LevelBean("special");
            // context should be clean - no openContext required
            cycle.collectAll(a); // should be context neutral
            // context should still be clean, again no openContext
            cycle.collectAll(d); // should be context neutral
            // context should still be clean, again no openContext
            cycle.collectAll(b);
            // and again
            cycle.collectAll(c);
            Map<String, LevelBean> map = new HashMap<String, LevelBean>();
            map.put("x", a);
            map.put("y", b);
            map.put("z", c);
            map.put("s", d);
            probeAllInstances(cycle, "map", map);
        }
    }

    private static final class LevelBean implements ObjectMetricsContext {

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
        public void switchToObjectContext(Tags context) {
            if (name.equals("special")) {
                context.tag(MetricsSource.TAG_TARGET, name);
            } else if (!name.isEmpty()) {
                context.instance(name);
            }
        }
    }

    @Test
    public void onlyProbesOfEnabledLevelsAreCollected() {
        register(new LevelDependentMetrics());
        setLevel(ProbeLevel.MANDATORY);
        assertCollectedCount(8); // 8 x mandatory
        assertMetric("mandatory", 1);
        setLevel(ProbeLevel.INFO);
        assertCollectedCount(16); // 8 x mandatory + info
        assertMetric("mandatory", 1);
        assertMetric("info", 2);
        setLevel(ProbeLevel.DEBUG);
        assertCollectedCount(24); // 8 x mandatory + info + debug
        assertMetric("mandatory", 1);
        assertMetric("info", 2);
        assertMetric("debug", 3);
    }

    private void assertMetric(String name, long value) {
        String i = MetricsSource.TAG_INSTANCE;
        String ns = MetricsSource.TAG_NAMESPACE;
        assertCollected(i + "=a " + name, value);
        assertCollected(i + "=b " + name, value);
        assertCollected(name, value);
        assertCollected("target=special " + name, value);
        assertCollected(ns + "=map " + i + "=a " + name, value);
        assertCollected(ns + "=map " + i + "=b " + name, value);
        assertCollected(ns + "=map " + i + "=z " + name, value);
        assertCollected(ns + "=map " + i + "=s target=special " + name, value);
    }

    private static class UpdatedMetrics {

        @Probe
        private int updates = 0;

        @BeforeCollectionCycle(value = 500, unit = TimeUnit.MILLISECONDS)
        private void update() {
            updates++;
        }

    }

    @Test
    public void updateOccursInSpecifiedCycleTime() {
        register(new UpdatedMetrics());
        assertCollected("updates", 1);
        assertCollected("updates", 1);
        sleepAtLeastMillis(501L);
        assertCollected("updates", 2);
        assertCollected("updates", 2);
    }

    private static final class RootService {

        @Probe
        long a;

        @ProbeSource
        SubService sub1;

        @ProbeSource
        SubService sub2;

        RootService(long a, SubService sub1, SubService sub2) {
            this.a = a;
            this.sub1 = sub1;
            this.sub2 = sub2;
        }
    }

    private static final class SubService implements MetricsSource, ObjectMetricsContext {

        static int n = 0;

        @Probe
        long b;

        @ProbeSource
        Worker defaultWorker;

        Worker alternativeDefaultWorker;

        Worker[] workers;

        String name;

        public SubService(long b, Worker... workers) {
            this.b = b;
            this.workers = workers;
            this.name = "sub" + (n++);
            this.defaultWorker = new Worker(10 + n, "worker.default");
            this.alternativeDefaultWorker = new Worker(10 + n, "no");
        }

        @Override
        public void switchToObjectContext(Tags context) {
            context.namespace(name);
        }

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.switchContext().namespace("worker.extra");
            cycle.collectAll(workers);
            cycle.switchContext().namespace("worker.default");
            cycle.collectAll(alternativeDefaultWorker);
        }
    }

    private static final class Worker implements ObjectMetricsContext {

        @Probe
        long c;
        @Probe
        long cFix = 42;

        private String name;

        Worker(long c, String name) {
            this.c = c;
            this.name = name + c;
        }

        @Override
        public void switchToObjectContext(Tags context) {
            if (name.startsWith("worker.default11")) {
                // this shows that ns of the parent can be replaced as long as no other tag was used
                context.namespace(name);
            } else {
                context.instance(name);
            }
        }

    }

    @Test
    public void nestedCollection() {
        register(new RootService(1,
                new SubService(2, new Worker(3, "no"), new Worker(4, "no")),
                new SubService(5, new Worker(6, "no"))));
        assertCollectedCount(17);
        assertCollected("a", 1);
        assertCollected("ns=sub0 b", 2);
        assertCollected("ns=sub1 b", 5);
        assertCollected("ns=worker.default11 c", 11);
        assertCollected("ns=worker.default11 cFix", 42);
        assertCollected("ns=sub1 instance=worker.default12 c", 12);
        assertCollected("ns=sub1 instance=worker.default12 cFix", 42);
        assertCollected("ns=worker.extra instance=no3 c", 3);
        assertCollected("ns=worker.extra instance=no3 cFix", 42);
        assertCollected("ns=worker.extra instance=no4 c", 4);
        assertCollected("ns=worker.extra instance=no4 cFix", 42);
        assertCollected("ns=worker.extra instance=no6 c", 6);
        assertCollected("ns=worker.extra instance=no6 cFix", 42);
        assertCollected("ns=worker.default instance=no11 c", 11);
        assertCollected("ns=worker.default instance=no11 cFix", 42);
        assertCollected("ns=worker.default instance=no12 c", 12);
        assertCollected("ns=worker.default instance=no12 cFix", 42);
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
