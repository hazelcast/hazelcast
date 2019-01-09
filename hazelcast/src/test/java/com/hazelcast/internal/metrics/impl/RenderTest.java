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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RenderTest {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);

        for (String name : metricsRegistry.getNames()) {
            ProbeInstance probeInstance = metricsRegistry.getProbeInstance(name);
            if (probeInstance != null && probeInstance.source != null) {
                metricsRegistry.deregister(probeInstance.source);
            }
        }
    }

    private void registerLongMetric(String name, final int value) {
        metricsRegistry.register(this, name, ProbeLevel.INFO,
                new LongProbeFunction<RenderTest>() {
                    @Override
                    public long get(RenderTest source) throws Exception {
                        return value;
                    }
                });
    }


    private void registerDoubleMetric(String name, final int value) {
        metricsRegistry.register(this, name, ProbeLevel.INFO,
                new DoubleProbeFunction<RenderTest>() {
                    @Override
                    public double get(RenderTest source) throws Exception {
                        return value;
                    }
                });
    }

    @Test(expected = NullPointerException.class)
    public void whenCalledWithNullRenderer() {
        metricsRegistry.render(null);
    }

    @Test
    public void whenLongProbeFunctions() {
        ProbeRenderer renderer = mock(ProbeRenderer.class);

        registerLongMetric("foo", 10);
        registerLongMetric("bar", 20);

        metricsRegistry.render(renderer);

        verify(renderer).renderLong("foo", 10);
        verify(renderer).renderLong("bar", 20);
        verifyNoMoreInteractions(renderer);
    }

    @Test
    public void whenDoubleProbeFunctions() {
        ProbeRenderer renderer = mock(ProbeRenderer.class);

        registerDoubleMetric("foo", 10);
        registerDoubleMetric("bar", 20);

        metricsRegistry.render(renderer);

        verify(renderer).renderDouble("foo", 10);
        verify(renderer).renderDouble("bar", 20);
        verifyNoMoreInteractions(renderer);
    }

    @Test
    public void whenException() {
        ProbeRenderer renderer = mock(ProbeRenderer.class);

        final ExpectedRuntimeException ex = new ExpectedRuntimeException();

        metricsRegistry.register(this, "foo", ProbeLevel.MANDATORY,
                new LongProbeFunction<RenderTest>() {
                    @Override
                    public long get(RenderTest source) throws Exception {
                        throw ex;
                    }
                });

        metricsRegistry.render(renderer);

        verify(renderer).renderException("foo", ex);
        verifyNoMoreInteractions(renderer);
    }

    @Test
    public void getSortedProbes_whenProbeAdded() {
        List<ProbeInstance> instances1 = metricsRegistry.getSortedProbeInstances();

        registerLongMetric("foo", 10);

        List<ProbeInstance> instances2 = metricsRegistry.getSortedProbeInstances();

        Assert.assertNotSame(instances1, instances2);
    }

    @Test
    public void getSortedProbes_whenNoChange() {
        List<ProbeInstance> instances1 = metricsRegistry.getSortedProbeInstances();

        List<ProbeInstance> instances2 = metricsRegistry.getSortedProbeInstances();

        assertSame(instances1, instances2);
    }
}
