/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.jet.impl.execution.SpecialBroadcastItem;
import com.hazelcast.jet.impl.execution.WatermarkCoalescer;

import java.util.Objects;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Watermark is an item occasionally inserted into a disordered
 * (sub)stream of timestamped items. The watermark's timestamp value has
 * the meaning "no items will follow with timestamp less than this." The
 * value of the watermark is always monotonically increasing in the
 * (sub)stream, with the effect of superimposing an ordered timestamp
 * sequence over the original disordered one. Watermark items are used by
 * windowing processors as anchoring points where the processor knows which
 * windows it can close and emit their aggregated results.
 *
 * @since Jet 3.0
 */
public final class Watermark implements SpecialBroadcastItem {

    private final long timestamp;

    /**
     * The watermark identifier distinguishes watermarks obtained from different sources.
     */
    private final byte key;

    /**
     * Constructs a new watermark item with {@code 0} watermark key.
     */
    public Watermark(long timestamp) {
        this.timestamp = timestamp;
        this.key = 0;
    }

    /**
     * Constructs a new watermark item with specified key.
     */
    public Watermark(long timestamp, byte key) {
        this.timestamp = timestamp;
        this.key = key;
    }

    /**
     * Returns the timestamp of this watermark item.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the key of this watermark item.
     */
    public byte key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Watermark
                && this.timestamp == ((Watermark) o).timestamp
                && this.key == ((Watermark) o).key;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, key);
    }

    @Override
    public String toString() {
        return timestamp ==
                WatermarkCoalescer.IDLE_MESSAGE_TIME
                        ? "Watermark{IDLE_MESSAGE}"
                        : "Watermark{ts=" + toLocalTime(timestamp)
                + ", key=" + key + "}";
    }
}
