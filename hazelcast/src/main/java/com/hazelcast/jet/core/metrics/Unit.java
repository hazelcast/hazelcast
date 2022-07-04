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

package com.hazelcast.jet.core.metrics;

/**
 * Measurement unit of user-defined metrics (see {@link Metric}). Meant to
 * provide further information on the type of value measured by user
 * metrics.
 * <p>
 * The unit values will end up populating the metric tag {@link
 * MetricTags#UNIT}. It can be used by UI tools to format the value and it's
 * not used by Jet itself.
 *
 * @since Jet 4.0
 */
public enum Unit {
    /** Size, counter, represented in bytes */
    BYTES,
    /** Timestamp or duration represented in ms */
    MS,
    /** An integer in range 0..100 */
    PERCENT,
    /** Number of items: size, counter... */
    COUNT,
    /** 0 or 1 */
    BOOLEAN,
    /** 0..n, ordinal of an enum */
    ENUM,
}
