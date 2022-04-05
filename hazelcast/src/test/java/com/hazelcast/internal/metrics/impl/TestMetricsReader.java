/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.ProbeFunction;

/**
 * Metrics reader for {@link LongProbeFunction} and {@link DoubleProbeFunction} metrics.
 */
public class TestMetricsReader {

    protected final MetricsRegistryImpl metricsRegistry;
    protected final String name;

    public TestMetricsReader(MetricsRegistryImpl metricsRegistry, String name) {
        this.metricsRegistry = metricsRegistry;
        this.name = name;
    }

    public Number read() throws Exception {
        ProbeInstance probeInstance = metricsRegistry.getProbeInstance(name);
        ProbeFunction function = probeInstance.function;
        Object source = probeInstance.source;

        if (function instanceof LongProbeFunction) {
            LongProbeFunction longFunction = (LongProbeFunction) function;
            return longFunction.get(source);
        } else if (function instanceof DoubleProbeFunction) {
            DoubleProbeFunction doubleFunction = (DoubleProbeFunction) function;
            return doubleFunction.get(source);
        } else {
            throw new IllegalStateException("Unexpected probe function type " + function.getClass().getName());
        }
    }
}
