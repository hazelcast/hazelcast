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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public class CapturingCollector implements MetricsCollector {

    private final Map<MetricDescriptor, Capture> captures = new HashMap<>();

    @Override
    public void collectLong(MetricDescriptor descriptor, long value) {
        captures.computeIfAbsent(DEFAULT_DESCRIPTOR_SUPPLIER.get().copy(descriptor), d -> new Capture())
                .capture(value);
    }

    @Override
    public void collectDouble(MetricDescriptor descriptor, double value) {
        captures.computeIfAbsent(DEFAULT_DESCRIPTOR_SUPPLIER.get().copy(descriptor), d -> new Capture())
                .capture(value);
    }

    @Override
    public void collectException(MetricDescriptor descriptor, Exception e) {
        // nop
    }

    @Override
    public void collectNoValue(MetricDescriptor descriptor) {
        // nop
    }

    public Map<MetricDescriptor, Capture> captures() {
        return unmodifiableMap(captures);
    }

    public boolean isCaptured(MetricDescriptor descriptor) {
        return captures.containsKey(descriptor);
    }

    public static final class Capture {
        private final List<Number> values = new LinkedList<>();
        private int hits = 0;

        private void capture(Number value) {
            values.add(value);
            hits++;
        }

        public int hits() {
            return hits;
        }

        public Number singleCapturedValue() {
            if (values.size() != 1) {
                throw new AssertionError("Expected to have only 1 value captured, but we captured " + values.size() + " "
                        + "values");
            }
            return values.get(0);
        }

        public List<Number> values() {
            return unmodifiableList(values);
        }
    }
}
