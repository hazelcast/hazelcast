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

import com.hazelcast.internal.metrics.ProbeFunction;

/**
 * Interface used for catching values of the dynamic metrics. The primary
 * use case for this interface is updating gauges with values of dynamic
 * metrics.
 */
interface MetricValueCatcher {
    /**
     * Catches metric value from a source object with a {@link ProbeFunction}.
     *
     * @param collectionId  The identifier of the metric collection cycle
     *                      calling this method
     * @param source        The object to read the value from
     * @param function      The probe function that reads the value from the
     *                      {@code source} object
     */
    void catchMetricValue(long collectionId, Object source, ProbeFunction function);

    /**
     * Catches long value.
     *
     * @param collectionId  The identifier of the metric collection cycle
     *                      calling this method
     * @param value         The value
     */
    void catchMetricValue(long collectionId, long value);

    /**
     * Catches double value.
     *
     * @param collectionId  The identifier of the metric collection cycle
     *                      calling this method
     * @param value         The value
     */
    void catchMetricValue(long collectionId, double value);
}
