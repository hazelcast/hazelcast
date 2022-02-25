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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.impl.util.Util;

import java.io.Serializable;
import java.util.Objects;

/**
 * A simple event with a timestamp and a sequence number.
 *
 * @since Jet 3.2
 */
public class SimpleEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long timestamp;
    private final long sequence;

    /**
     * Create an event with the given timestamp and sequence number
     */
    public SimpleEvent(long timestamp, long sequence) {
        this.timestamp = timestamp;
        this.sequence = sequence;
    }

    /**
     * Timestamp of the event in milliseconds
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Sequence number of the event
     */
    public long sequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "SimpleEvent(" +
            "timestamp=" + Util.toLocalTime(timestamp) +
            ", sequence=" + sequence +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleEvent that = (SimpleEvent) o;
        return timestamp == that.timestamp &&
            sequence == that.sequence;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sequence);
    }
}
