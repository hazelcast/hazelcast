/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable data class containing information about one metric measurement,
 * consisting of:
 * <ul>
 * <li>metric value</li>
 * <li>metric timestamp, generated when the metric was gathered</li>
 * <li>metric descriptor (set of tag name-value pairs) </li>
 * </ul>
 * <p>
 * A metrics descriptor can be thought of as a set of attributes associated
 * with a particular metric, metric which in turn is defined by its name
 * (for a full list of metric names provided see {@link MetricNames}).
 * The attributes are specified as tags that have names and values (for a
 * full list of tag names see {@link MetricTags}). An example
 * descriptor would have a collection of tags/attributes like this:
 * {@code module=jet, job=jobId, exec=execId, vertex=filter, proc=3,
 * unit=count, metric=queuesCapacity, ...}
 *
 * @since 3.2
 */
public final class Measurement implements IdentifiedDataSerializable {

    // tag name -> tag value
    private Map<String, String> tags;
    private String metric;
    private long value;
    private long timestamp;

    // needed for deserialization
    Measurement() {
    }

    private Measurement(String metric, long value, long timestamp, @Nonnull Map<String, String> tags) {
        this.metric = metric;
        this.value = value;
        this.timestamp = timestamp;
        this.tags = new HashMap<>(tags);
    }

    /**
     * Builds a {@link Measurement} instance based on timestamp, value and
     * the metric descriptor in map form.
     */
    @Nonnull
    public static Measurement of(
            @Nonnull String metric, long value, long timestamp, @Nonnull Map<String, String> tags
    ) {
        Objects.requireNonNull(tags, "metric");
        Objects.requireNonNull(tags, "tags");
        return new Measurement(metric, value, timestamp, tags);
    }

    /**
     * Returns the value associated with this {@link Measurement}.
     */
    public long value() {
        return value;
    }

    /**
     * Returns the timestamps associated with this {@link Measurement}, the
     * moment when the value was gathered.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the name of the metric. For a list of different metrics
     * see {@link MetricNames}.
     */
    @Nonnull
    public String metric() {
        return metric;
    }

    /**
     * Returns the value associated with a specific tag, based on the metric
     * description of this particular {@link Measurement}. For a list of
     * possible tag names see {@link MetricTags}.
     */
    @Nullable
    public String tag(String name) {
        return tags.get(name);
    }

    @Override
    public int getFactoryId() {
        return MetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return MetricsDataSerializerHook.MEASUREMENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(tags);
        out.writeLong(value);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tags = in.readObject();
        value = in.readLong();
        timestamp = in.readLong();
    }

    @Override
    public int hashCode() {
        return 31 * (int) (timestamp * 31 + value) + Objects.hashCode(tags);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Measurement)) {
            return false;
        }
        final Measurement that = (Measurement) obj;
        return this.timestamp == that.timestamp
                && this.value == that.value
                && Objects.equals(this.tags, that.tags);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("%s %,5d", metric, value))
                .append(" ")
                .append(Util.toLocalTime(timestamp))
                .append(" [");

        String tags = this.tags.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));
        sb.append(tags).append(']');

        return sb.toString();
    }
}
