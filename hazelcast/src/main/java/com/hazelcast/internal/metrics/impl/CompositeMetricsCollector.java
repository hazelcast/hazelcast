/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

public class CompositeMetricsCollector implements MetricsCollector {

    private final MetricsCollector[] collectors;

    public CompositeMetricsCollector(MetricsCollector... collectors) {
        this.collectors = collectors;
    }

    @Override
    public void collectLong(MetricDescriptor descriptor, long value) {
        for (MetricsCollector collector : collectors) {
            collector.collectLong(descriptor, value);
        }
    }

    @Override
    public void collectDouble(MetricDescriptor descriptor, double value) {
        for (MetricsCollector collector : collectors) {
            collector.collectDouble(descriptor, value);
        }
    }

    @Override
    public void collectException(MetricDescriptor descriptor, Exception e) {
        for (MetricsCollector collector : collectors) {
            collector.collectException(descriptor, e);
        }
    }

    @Override
    public void collectNoValue(MetricDescriptor descriptor) {
        for (MetricsCollector collector : collectors) {
            collector.collectNoValue(descriptor);
        }
    }
}
