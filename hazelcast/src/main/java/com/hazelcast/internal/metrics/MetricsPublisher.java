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

package com.hazelcast.internal.metrics;

/**
 * Represents an object which publishes a set of metrics to some destination.
 */
public interface MetricsPublisher {

    /**
     * Publish the given metric with a long value.
     */
    void publishLong(MetricDescriptor descriptor, long value);

    /**
     * Publish the given metric with a double value.
     */
    void publishDouble(MetricDescriptor descriptor, double value);

    /**
     * Callback is called after all metrics are published for a given
     * metric collection round.
     */
    default void whenComplete() {
    }

    /**
     * Perform any necessary cleanup before shutdown.
     */
    default void shutdown() {
    }

    /**
     * Name of the publisher, only used for debugging purposes.
     */
    String name();

}
