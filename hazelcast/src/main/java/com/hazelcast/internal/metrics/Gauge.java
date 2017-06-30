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

package com.hazelcast.internal.metrics;

/**
 * A Gauge is {@link Metric} where a particular value is read instantaneous. E.g. the current size of the
 * pending operations queue.
 *
 * The Gauge interface has not methods for retrieving the actual value, because it depends on the type of
 * value to read.
 *
 * @see LongGauge
 * @see DoubleGauge
 */
public interface Gauge extends Metric {

    /**
     * Checks if this Gauge is bound to a probe.
     *
     * A probe can start later than the Gauge and it could be destroyed while the Gauge is still is in use.
     *
     * @return true if this Gauge is bound to a probe.
     */
    boolean isBound();

    /**
     * Renders the value from the gauge.
     *
     * @param stringBuilder the {@link StringBuilder} to write to.
     * @param unboundValue the value to render when the gauge isn't bound.
     */
    void render(StringBuilder stringBuilder, String unboundValue);
}
