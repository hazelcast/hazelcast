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
 * A Gauge is a {@link Metric} where a particular value is read instantaneously.
 * E.g. the current size of the pending operations queue.
 *
 * The Gauge interface has no methods for retrieving the actual value,
 * because it depends on the type of value to read.
 *
 * @see LongGauge
 * @see DoubleGauge
 */
public interface Gauge extends Metric {

    /**
     * Renders the value from the gauge.
     *
     * @param stringBuilder the {@link StringBuilder} to write to.
     */
    void render(StringBuilder stringBuilder);
}
