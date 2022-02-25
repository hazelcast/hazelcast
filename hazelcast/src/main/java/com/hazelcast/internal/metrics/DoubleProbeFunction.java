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
 * A {@link ProbeFunction} that provides a double value and can be used to
 * create a probe using {@link MetricsRegistry#registerStaticProbe(Object, String, ProbeLevel, LongProbeFunction)}
 *
 * @param <S> the type of the source object.
 * @see LongProbeFunction
 */
public interface DoubleProbeFunction<S> extends ProbeFunction {

    /**
     * Gets the current value of the source object.
     *
     * @param source the source object.
     * @return the current value of the source object.
     * @throws Exception if something fails while getting the value.
     */
    double get(S source) throws Exception;
}
