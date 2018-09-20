/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics.management;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.metrics.management.ManagementCenterPublisher.decompressingIterator;

public class MetricsResultSet  {

    private final long nextSequence;
    private final List<MetricsCollection> collections;

    public MetricsResultSet(ConcurrentArrayRingbuffer.RingbufferSlice<Map.Entry<Long, byte[]>> slice) {
        this.nextSequence = slice.nextSequence();
        this.collections = slice.stream()
                .map(e -> new MetricsCollection(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * The next sequence to read from.
     */
    public long nextSequence() {
        return nextSequence;
    }

    public List<MetricsCollection> collections() {
        return collections;
    }

    /**
     * Deserializing iterator for reading metrics
     */
    public static class MetricsCollection implements Iterable<Metric> {

        private final long timestamp;
        private final byte[] bytes;

        public MetricsCollection(long timestamp, byte[] bytes) {
            this.timestamp = timestamp;
            this.bytes = bytes;
        }

        public long timestamp() {
            return timestamp;
        }

        public int sizeInBytes() {
            return bytes.length;
        }

        @Nonnull
        @Override
        public Iterator<Metric> iterator() {
            return decompressingIterator(bytes);
        }
    }
}

