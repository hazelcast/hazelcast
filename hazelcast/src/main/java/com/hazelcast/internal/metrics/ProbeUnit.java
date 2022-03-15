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

import com.hazelcast.internal.metrics.impl.MetricsCompressor;

/**
 * Measurement unit of a Probe. Not used on member, becomes a part of the key.
 * <p>
 * <b>NOTE</b>: The enum ordinals are used by {@link MetricsCompressor},
 * therefore, the ordinal values should remain stable between versions to
 * ensure version compatibility.
 * <p>
 * When modifying this enum, please be aware of the following rules
 * <ul>
 *     <li>do not reorder the values of this enum,
 *     <li>do not delete any value from this enum,
 *     <li>add new values at the end of the list,
 *     <li>when introducing a new enum value, mark the value as new by
 *     setting {{@link #newUnit}} to {@code true} and add the version
 *     dependent RU_COMPAT_X_Y.
 * </ul>
 * <p>
 */
public enum ProbeUnit {
    /** Size, counter, represented in bytes */
    BYTES,
    /** Timestamp or duration represented in ms */
    MS,
    /** Timestamp or duration represented in nanoseconds */
    NS,
    /** An integer mostly in range 0..100 or a double mostly in range 0..1 */
    PERCENT,
    /** Number of items: size, counter... */
    COUNT,
    /** 0 or 1 */
    BOOLEAN,
    /** 0..n, ordinal of an enum */
    ENUM,
    /** Timestamp or duration represented in µs */
    US;

    /**
     * Setting to {@code true} indicates that when compressing a metric with this
     * unit, the unit should be converted to a tag to ensure backwards compatibility.
     */
    private final boolean newUnit;

    ProbeUnit() {
        this(false);
    }

    ProbeUnit(boolean newUnit) {
        this.newUnit = newUnit;
    }

    public boolean isNewUnit() {
        return newUnit;
    }
}
