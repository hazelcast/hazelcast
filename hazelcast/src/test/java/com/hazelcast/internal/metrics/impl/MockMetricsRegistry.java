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

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;

/**
 * A {@link MetricsRegistry} that wraps a real implementation and allows to
 * setup results of {@link LongGauge} and {@link DoubleGauge} for testing.
 */
public final class MockMetricsRegistry implements MetricsRegistry {

    private final MetricsRegistry registry;
    private final Map<String, Long> longMetrics = new HashMap<String, Long>();
    private final Map<String, Double> doubleMetrics = new HashMap<String, Double>();

    public MockMetricsRegistry(MetricsRegistry registry) {
        this.registry = registry;
    }

    public void mockDouble(String name, double value) {
        doubleMetrics.put(name, value);
    }

    public void mockLong(String name, long value) {
        longMetrics.put(name, value);
    }

    @Override
    public LongGauge newLongGauge(final String name) {
        return new LongGauge() {
            final LongGauge gauge = registry.newLongGauge(name);

            @Override
            public long read() {
                Long val = longMetrics.get(name);
                return val != null ? val.longValue() : gauge.read();
            }
        };
    }

    @Override
    public DoubleGauge newDoubleGauge(final String name) {
        return new DoubleGauge() {
            final DoubleGauge gauge = registry.newDoubleGauge(name);

            @Override
            public double read() {
                Double val = doubleMetrics.get(name);
                return val != null ? val.doubleValue() : gauge.read();
            }
        };
    }

    @Override
    public void register(Object root) {
        registry.register(root);
    }

    @Override
    public CollectionContext openContext(ProbeLevel level) {
        return registry.openContext(level);
    }

}
