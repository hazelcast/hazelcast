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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricSource;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class MetricsRegistry2Test extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void test() {
        metricsRegistry.register(new Foo());

        metricsRegistry.collect(new MyMetricsCollector(), ProbeLevel.INFO);
    }


    @Test
    public void testDynamic() {
        metricsRegistry.register(new Dynamic());

        metricsRegistry.collect(new MyMetricsCollector(), ProbeLevel.INFO);
    }

    @Test
    public void testRealCluster() {
        TestHazelcastInstanceFactory f = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
//        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
//        HazelcastInstance hz3 =Hazelcast.newHazelcastInstance();
        Node node = getNode(hz);
        MetricsRegistryImpl metricsRegistry = (MetricsRegistryImpl) node.getNodeEngine().getMetricsRegistry();


        IMap map = hz.getMap("employees");
        map.put("foo", "bar");

        System.out.println("number of roots:" + metricsRegistry.getNames().size());
        for (String name : metricsRegistry.getNames()) {
            System.out.println(name);
        }

        System.out.println("------------------------------------------------");
//        boolean contains = metricsRegistry.getNames().contains("tcp");
//        System.out.println(contains);

        // for (; ; ) {
        metricsRegistry.collect(new MyMetricsCollector(), ProbeLevel.INFO);
        //}
    }

    public static class Bar {
        @Probe(name = "a")
        private int a;
        @Probe(name = "b")
        private AtomicLong b = new AtomicLong();

    }

    public static class Foo implements MetricSource {
        @Probe(name = "a")
        private int a;
        @Probe(name = "b")
        private AtomicLong b = new AtomicLong();

        private Bar bar = new Bar();

        @Override
        public void collectMetrics(CollectionCycle cycle) {
            cycle.collect("bar", bar);
        }
    }

    public static class Dynamic implements MetricSource {
        @Probe(name = "a")
        private int a;
        @Probe(name = "b")
        private AtomicLong b = new AtomicLong();

        private Foo foo1 = new Foo();
        private Foo foo2 = new Foo();

        @Override
        public void collectMetrics(CollectionCycle cycle) {
            cycle.collect("foo1", foo1);
            cycle.collect("foo2", foo2);
        }
    }

    private static class MyMetricsCollector implements MetricsCollector {
        @Override
        public void collectLong(CharSequence name, long value) {
            System.out.println(name + "=" + value);
        }

        @Override
        public void collectDouble(CharSequence name, double value) {
            System.out.println(name + "=" + value);
        }

    }
}
