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
 * Each metric is uniquely identified using a name. See {@link #getName()}.
 *
 * A metric gets its content from Probes. An input can be one of the following:
 * <ol>
 *     <li>a method with the {@link Probe} annotation</li>
 *     <li>a field with the {@link Probe} annotation</li>
 *     <li>a {@link LongProbe} function</li>
 *     <li>a {@link DoubleProbe} function</li>
 * </ol>
 *
 * If the Metric is obtained before it is registered, the Metric is without input. As soon as the registration happens, the
 * input is set on the metric and the current information will be retrieved.
 */
public interface Metric {

    /**
     * Gets the name that identifies this metric. The returned value will never change and never be null.
     *
     * @return the name of the metric.
     */
    String getName();

    /**
     * Renders the metric.
     *
     * @param sb the StringBuilder to write to.
     * @throws NullPointerException if sb is null
     */
    void render(StringBuilder sb);
}
