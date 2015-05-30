/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * A gauge metric is an instantaneous reading of a particular value: for example, the current size of the pending
 * operations queue.
 *
 * A Gauge can be used before it is registered and no input/source is set. In this case, the {@link #readDouble()} and
 * {@link #readDouble()} return 0.
 *
 * A Gauge can be used after the {@link MetricsRegistry#deregister(Object)} is called. In this case, the {@link #readDouble()} and
 * {@link #readDouble()} return 0.
 */
public interface Gauge extends Metric {

    /**
     * Reads the current available (gauge metric) value as a long.
     *
     * If the underlying metric input providing a floating point value, then the value will be rounded using
     * {@link Math#round(double)}.
     *
     * If no input is available, or there are problems obtaining a value from the input, 0 is returned.
     *
     * @return the current available (gauge metric) value as a long.
     */
    long readLong();

    /**
     * Reads the current available (gauge metric) value as a double.
     *
     * If the underlying metric input doesn't provide a floating point value, then the value will be converted to
     * a floating point value.
     *
     * If no input is available, or there are problems obtaining a value from the input, 0 is returned.
     *
     * @return the current available (gauge metric) value as a double.
     */
    double readDouble();
}
