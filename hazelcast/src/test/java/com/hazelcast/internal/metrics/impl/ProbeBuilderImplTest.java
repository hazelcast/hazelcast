/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.logging.Logger;
import org.junit.Test;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ProbeBuilderImplTest {

    @Probe
    private long probe1;

    @Probe(name = "secondProbe", level = ProbeLevel.MANDATORY, unit = ProbeUnit.BYTES)
    private long probe2;

    @Test
    public void test_scanAndRegister() {
        MetricsRegistryImpl mr = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        mr.newProbeBuilder()
          .withTag("tag1", "value1")
          .scanAndRegister(this);

        assertEquals(new HashSet<String>(asList(
                "[tag1=value1,unit=count,metric=probe1]",
                "[tag1=value1,unit=bytes,metric=secondProbe]"
        )), mr.getNames());
    }

    @Test
    public void test_register() {

    }
}
