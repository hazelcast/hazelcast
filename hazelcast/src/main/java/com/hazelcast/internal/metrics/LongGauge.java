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
 * A LongGauge is a {@link Gauge} where a particular long value is read
 * instantaneously. E.g. the current size of the pending operations queue.
 *
 * {@link DoubleGauge}
 */
public interface LongGauge extends Gauge {

    /**
     * Reads the current available value as a long.
     *
     * If the underlying probe provides a double value, then the value will be
     * rounded using {@link Math#round(double)}.
     *
     * If no probe is available, or there are problems obtaining a value from
     * the probe, 0 is returned.
     *
     * @return the current value.
     */
    long read();
}
