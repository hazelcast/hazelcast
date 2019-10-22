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

package com.hazelcast.internal.metrics.managementcenter;

import com.hazelcast.internal.metrics.MetricDescriptor;

import static com.hazelcast.internal.metrics.managementcenter.MetricsCompressor.ValueType;

/**
 * Represents a metric data point (descriptor and value).
 */
public final class Metric {

    private final MetricDescriptor descriptor;
    private final ValueType type;
    private long longValue;
    private double doubleValue;

    Metric(MetricDescriptor descriptor, ValueType type, long value) {
        this.descriptor = descriptor;
        this.type = type;
        this.longValue = value;
    }

    Metric(MetricDescriptor descriptor, ValueType type, double value) {
        this.descriptor = descriptor;
        this.type = type;
        this.doubleValue = value;
    }

    /**
     * Consumes the Metric with the given {@link MetricConsumer}.
     *
     * @param consumer metric consumer
     */
    public void provide(MetricConsumer consumer) {
        switch (type) {
            case LONG:
                consumer.consumeLong(descriptor, longValue);
                break;
            case DOUBLE:
                consumer.consumeDouble(descriptor, doubleValue);
                break;
            default:
                throw new IllegalStateException("Unexpected metric value type: " + type);
        }
    }
}
