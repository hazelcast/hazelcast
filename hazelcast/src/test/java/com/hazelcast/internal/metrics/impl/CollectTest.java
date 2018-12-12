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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CollectTest {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));

        for (String name : metricsRegistry.getNames()) {
            ProbeInstance probeInstance = metricsRegistry.getProbeInstance(name);
            if (probeInstance != null && probeInstance.source != null) {
                metricsRegistry.deregister(probeInstance.source);
            }
        }
    }

    private void registerLongMetric(String name, final int value) {
        metricsRegistry.register(this, name, ProbeLevel.INFO,
                new LongProbeFunction<CollectTest>() {
                    @Override
                    public long get(CollectTest source) {
                        return value;
                    }
                });
    }


    private void registerDoubleMetric(String name, final int value) {
        metricsRegistry.register(this, name, ProbeLevel.INFO,
                new DoubleProbeFunction<CollectTest>() {
                    @Override
                    public double get(CollectTest source) {
                        return value;
                    }
                });
    }

    @Test(expected = NullPointerException.class)
    public void whenCalledWithNullCollector() {
        metricsRegistry.collect(null, ProbeLevel.INFO);
    }


    @Test(expected = NullPointerException.class)
    public void whenCalledWithNullProbeLevel() {
        metricsRegistry.collect(mock(MetricsCollector.class), null);
    }


    @Test
    public void whenLongProbeFunctions() {
        MetricsCollector collector = mock(MetricsCollector.class);

        registerLongMetric("foo", 10);
//        registerLongMetric("bar", 20);

        metricsRegistry.collect(collector, ProbeLevel.INFO);

        verify(collector).collectLong("foo", 10);
//        verify(collector).collectLong("bar", 20);
        verifyNoMoreInteractions(collector);
    }

    @Test
    public void whenDoubleProbeFunctions() {
        MetricsCollector collector = mock(MetricsCollector.class);

        registerDoubleMetric("foo", 10);
        registerDoubleMetric("bar", 20);

        metricsRegistry.collect(collector, ProbeLevel.INFO);

        verify(collector).collectDouble("foo", 10);
        verify(collector).collectDouble("bar", 20);
        verifyNoMoreInteractions(collector);
    }

    @Test
    public void whenException() {
        MetricsCollector collector = mock(MetricsCollector.class);

        final ExpectedRuntimeException ex = new ExpectedRuntimeException();

        metricsRegistry.register(this, "foo", ProbeLevel.MANDATORY,
                new LongProbeFunction<CollectTest>() {
                    @Override
                    public long get(CollectTest source) throws Exception {
                        throw ex;
                    }
                });

        metricsRegistry.collect(collector, ProbeLevel.INFO);

        //verify(collector).collectException("foo", ex);
        verifyNoMoreInteractions(collector);
    }

//    @Test
//    public void getSortedProbes_whenProbeAdded() {
//        List<ProbeInstance> instances1 = metricsRegistry.getSortedProbeInstances();
//
//        registerLongMetric("foo", 10);
//
//        List<ProbeInstance> instances2 = metricsRegistry.getSortedProbeInstances();
//
//        Assert.assertNotSame(instances1, instances2);
//    }
//
//    @Test
//    public void getSortedProbes_whenNoChange() {
//        List<ProbeInstance> instances1 = metricsRegistry.getSortedProbeInstances();
//
//        List<ProbeInstance> instances2 = metricsRegistry.getSortedProbeInstances();
//
//        assertSame(instances1, instances2);
//    }
}
