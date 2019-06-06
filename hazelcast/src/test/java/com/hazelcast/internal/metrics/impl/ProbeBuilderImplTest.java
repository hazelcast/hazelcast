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

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeBuilder;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProbeBuilderImplTest {

    @Probe
    private int probe1 = 1;

    @Probe(name = "secondProbe", level = ProbeLevel.MANDATORY, unit = ProbeUnit.BYTES)
    private int probe2 = 2;

    @Probe(level = ProbeLevel.DEBUG)
    private int probe3 = 3;

    @Test
    public void test_scanAndRegister() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        registry.newProbeBuilder()
          .withTag("tag1", "value1")
          .scanAndRegister(this);

        assertProbes(registry);
    }

    @Test
    public void test_register() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        ProbeBuilder builder = registry.newProbeBuilder()
                                       .withTag("tag1", "value1");
        builder.register(this, "probe1", ProbeLevel.INFO, ProbeUnit.COUNT,
                (LongProbeFunction<ProbeBuilderImplTest>) source -> source.probe1);
        builder.register(this, "secondProbe", ProbeLevel.INFO, ProbeUnit.BYTES,
                (LongProbeFunction<ProbeBuilderImplTest>) source -> source.probe2);

        assertProbes(registry);
    }

    private void assertProbes(MetricsRegistryImpl registry) {
        final String p1Name = "[tag1=value1,unit=count,metric=probe1]";
        final String p2Name = "[tag1=value1,unit=bytes,metric=secondProbe]";
        assertEquals(new HashSet<>(asList(p1Name, p2Name)), registry.getNames());

        registry.render(new ProbeRenderer() {
            @Override
            public void renderLong(String name, long value) {
                if (p1Name.equals(name)) {
                    assertEquals(probe1, value);
                } else if (p2Name.equals(name)) {
                    assertEquals(probe2, value);
                } else {
                    fail("Unknown metric: " + name);
                }
            }

            @Override
            public void renderDouble(String name, double value) {
                fail("Unknown metric: " + name);
            }

            @Override
            public void renderException(String name, Exception e) {
                throw new RuntimeException(e);
            }

            @Override
            public void renderNoValue(String name) {
                fail("Unknown metric: " + name);
            }
        });
    }
}
