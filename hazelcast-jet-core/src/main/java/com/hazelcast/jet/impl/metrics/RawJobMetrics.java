/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class RawJobMetrics implements IdentifiedDataSerializable {

    private long timestamp;
    private Map<String, Long> values;

    RawJobMetrics() { //needed for deserialization
    }

    private RawJobMetrics(long timestamp, @Nonnull Map<String, Long> values) {
        Objects.requireNonNull(values, "values");
        this.timestamp = timestamp;
        this.values = Collections.unmodifiableMap(values);
    }

    public static RawJobMetrics empty() {
        return RawJobMetrics.of(System.currentTimeMillis(), Collections.emptyMap());
    }

    public static RawJobMetrics of(@Nonnull Map<String, Long> values) {
        return RawJobMetrics.of(System.currentTimeMillis(), values);
    }

    public static RawJobMetrics of(long timestamp, @Nonnull Map<String, Long> values) {
        return new RawJobMetrics(timestamp, values);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Long> getValues() {
        return values;
    }

    public RawJobMetrics prefixNames(String prefix) {
        return new RawJobMetrics(
                timestamp,
                values.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> JobMetricsUtil.addPrefixToDescriptor(e.getKey(), prefix),
                                        Map.Entry::getValue
                                )
                        )
        );
    }

    @Override
    public int getFactoryId() {
        return JetMetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetMetricsDataSerializerHook.RAW_JOB_METRICS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(values);
        out.writeLong(timestamp);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        values = in.readObject();
        timestamp = in.readLong();
    }

    @Override
    public int hashCode() {
        return (int) timestamp * 31 + Objects.hashCode(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        RawJobMetrics that;
        return Objects.equals(values, (that = (RawJobMetrics) obj).values)
                && this.timestamp == that.timestamp;
    }

    @Override
    public String toString() {
        return values + " @ " + timestamp;
    }
}
