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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * An immutable collection of job-specific metrics, pairs of metric names
 * and sets of associated {@link MeasurementImpl}s.
 *
 * @since 3.2
 */
public final class JobMetricsImpl implements JobMetrics, IdentifiedDataSerializable {

    private static final JobMetricsImpl EMPTY = new JobMetricsImpl(Collections.emptyMap());

    private static final Collector<MeasurementImpl, ?, Map<String, List<MeasurementImpl>>> COLLECTOR =
        Collectors.groupingBy(MeasurementImpl::metric);

    //metric name -> set of measurements
    private Map<String, List<MeasurementImpl>> metrics;

    JobMetricsImpl() {
    }

    private JobMetricsImpl(@Nonnull Map<String, List<MeasurementImpl>> metrics) {
        this.metrics = new HashMap<>(metrics);
    }

    /**
     * Returns an empty {@link JobMetricsImpl} object.
     */
    @Nonnull
    @PrivateApi
    public static JobMetricsImpl empty() {
        return EMPTY;
    }

    /**
     * Builds a {@link JobMetricsImpl} object based on a map of
     * {@link MeasurementImpl}s.
     */
    @Nonnull
    @PrivateApi
    public static JobMetricsImpl of(@Nonnull Map<String, List<MeasurementImpl>> metrics) {
        return new JobMetricsImpl(metrics);
    }

    /**
     * Returns all metrics present.
     */
    @Nonnull
    public Set<String> metrics() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    /**
     * Returns all {@link MeasurementImpl}s associated with a given metric name.
     * <p>
     * For a list of job-specific metric names please see {@link MetricNames}.
     */
    @Nonnull
    public List<MeasurementImpl> get(@Nonnull String metricName) {
        Objects.requireNonNull(metricName);
        List<MeasurementImpl> measurements = metrics.get(metricName);
        return measurements == null ? Collections.emptyList() : measurements;
    }

    /**
     * Convenience method for {@link #filter(Predicate< MeasurementImpl >)},
     * returns a new {@link JobMetricsImpl} instance containing only those
     * {@link MeasurementImpl}s which have the specified tag set to the
     * specified value.
     * <p>
     * For a list of available tag names, see {@link MetricTags}.
     */
    @Nonnull
    public JobMetricsImpl filter(@Nonnull String tagName, @Nonnull String tagValue) {
        return filter(MeasurementPredicates.tagValueEquals(tagName, tagValue));
    }

    /**
     * Returns a new {@link JobMetricsImpl} instance containing a subset of
     * the {@link MeasurementImpl}s found in the current one. The subset is
     * formed by those {@link MeasurementImpl}s which match the provided
     * {@link Predicate}.
     * <p>
     * The metric names which have all their {@link MeasurementImpl}s filtered
     * out won't be present in the new {@link
     * JobMetricsImpl} instance.
     */
    @Nonnull
    public JobMetricsImpl filter(@Nonnull Predicate<Measurement> predicate) {
        Objects.requireNonNull(predicate, "predicate");

        Map<String, List<MeasurementImpl>> filteredMetrics =
            metrics.values().stream()
                   .flatMap(List::stream)
                   .filter(predicate)
                   .collect(COLLECTOR);
        return new JobMetricsImpl(filteredMetrics);
    }

    @Override
    public int getFactoryId() {
        return MetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return MetricsDataSerializerHook.JOB_METRICS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(metrics);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        metrics = in.readObject();
    }

    @Override
    public int hashCode() {
        return metrics.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        return Objects.equals(metrics, ((JobMetricsImpl) obj).metrics);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        metrics.entrySet().stream()
            .sorted(Comparator.comparing(Entry::getKey))
            .forEach(mainEntry -> {
                sb.append(mainEntry.getKey()).append(":\n");
                mainEntry.getValue().stream()
                    .collect(groupingBy(m -> {
                        String vertex = m.tag(MetricTags.VERTEX);
                        return vertex == null ? "" : vertex;
                    }))
                    .entrySet().stream()
                    .sorted(Comparator.comparing(Entry::getKey))
                    .forEach(e -> {
                        String vertexName = e.getKey();
                        sb.append("  ").append(vertexName).append(":\n");
                        e.getValue().forEach(m -> sb.append("    ").append(m).append("\n"));
                    });
            });
        return sb.toString();
    }
}
