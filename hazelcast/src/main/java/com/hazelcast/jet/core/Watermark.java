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

package com.hazelcast.jet.core;

import com.hazelcast.jet.impl.execution.BroadcastItem;

import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
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
public final class Watermark implements BroadcastItem {

    private final long timestamp;

    /**
     * Constructs a new watermark item.
     */
    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns the timestamp of this watermark item.
     */
    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof Watermark && this.timestamp == ((Watermark) o).timestamp;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(timestamp);
    }

    @Override
    public String toString() {
        return timestamp == IDLE_MESSAGE.timestamp
                ? "Watermark{IDLE_MESSAGE}"
                : "Watermark{ts=" + toLocalTime(timestamp) + '}';
    }
}
