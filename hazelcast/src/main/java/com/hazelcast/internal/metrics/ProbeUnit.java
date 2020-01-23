/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Measurement unit of a Probe. Not used on member, becomes a part of the key.
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
}
