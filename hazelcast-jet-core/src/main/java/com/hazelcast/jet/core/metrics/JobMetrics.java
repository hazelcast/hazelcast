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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * An immutable collection of job-specific metrics, pairs of metric names
 * and sets of associated {@link Measurement}s.
 *
 * @since 3.2
 */
public final class JobMetrics implements IdentifiedDataSerializable {

    private static final JobMetrics EMPTY = new JobMetrics(Collections.emptyMap());

    private static final Collector<Measurement, ?, TreeMap<String, List<Measurement>>> COLLECTOR = Collectors.groupingBy(
            measurement -> measurement.getTag(MetricTags.METRIC),
            TreeMap::new,
            Collectors.mapping(identity(), toList())
    );

    private Map<String, List<Measurement>> metrics; //metric name -> set of measurements

    JobMetrics() { //needed for deserialization
    }

    private JobMetrics(@Nonnull Map<String, List<Measurement>> metrics) {
        this.metrics = new HashMap<>(metrics);
    }

    /**
     * Returns an empty {@link JobMetrics} object.
     */
    @Nonnull
    public static JobMetrics empty() {
        return EMPTY;
    }

    /**
     * Builds a {@link JobMetrics} object based on one global timestamp and
     * a key-value map of raw metrics data. The key {@code String}s in the
     * map should be well formed metric descriptors and the values
     * associated with them are {@code long} numbers.
     * <p>
     * Descriptors are {@code String}s structured as a comma separated lists
     * of tag=value pairs, enclosed in square brackets. An example of a
     * valid metric descriptor would be:
     * <pre>{@code
     *      [module=jet,job=jobId,exec=execId,vertex=filter,proc=3,
     *                                   unit=count,metric=queuesCapacity]
     * }</pre>
     */
    @Nonnull
    public static JobMetrics of(long timestamp, @Nonnull Map<String, Long> metrics) {
        Objects.requireNonNull(metrics, "metrics");
        if (metrics.isEmpty()) {
            return EMPTY;
        }
        return new JobMetrics(parseRawMetrics(timestamp, metrics));
    }

    private static Map<String, List<Measurement>> parseRawMetrics(long timestamp, Map<String, Long> raw) {
        HashMap<String, List<Measurement>> parsed = new HashMap<>();
        for (Entry<String, Long> rawEntry : raw.entrySet()) {
            Long value = rawEntry.getValue();
            if (value == null) {
                throw new IllegalArgumentException("Value missing");
            }

            String descriptor = rawEntry.getKey();
            Map<String, String> tags = JobMetricsUtil.parseMetricDescriptor(descriptor);

            String metricName = tags.get(MetricTags.METRIC);
            if (metricName == null || metricName.isEmpty()) {
                throw new IllegalArgumentException("Metric name missing");
            }

            List<Measurement> measurements = parsed.computeIfAbsent(metricName, mn -> new ArrayList<>());
            measurements.add(Measurement.of(value, timestamp, tags));
        }
        return parsed;
    }

    /**
     * Returns all metrics present.
     */
    @Nonnull
    public Set<String> metrics() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    /**
     * Returns all {@link Measurement}s associated with a given metric name.
     * <p>
     * For a list of job-specific metric names please see {@link MetricNames}.
     */
    @Nonnull
    public List<Measurement> get(@Nonnull String metricName) {
        Objects.requireNonNull(metricName);
        List<Measurement> measurements = metrics.get(metricName);
        return measurements == null ? Collections.emptyList() : measurements;
    }

    /**
     * Convenience method for {@link #filter(Predicate<Measurement>)},
     * returns a new {@link JobMetrics} instance containing only those
     * {@link Measurement}s which have the specified tag set to the
     * specified value.
     * <p>
     * For a list of available tag names, see {@link MetricTags}.
     */
    @Nonnull
    public JobMetrics filter(@Nonnull String tagName, @Nonnull String tagValue) {
        return filter(MeasurementPredicates.tagValueEquals(tagName, tagValue));
    }

    /**
     * Returns a new {@link JobMetrics} instance containing a subset of
     * the {@link Measurement}s found in the current one. The subset is
     * formed by those {@link Measurement}s which match the provided
     * {@link Predicate}.
     * <p>
     * The metric names which have all their {@link Measurement}s filtered
     * out won't be present in the new {@link
     * JobMetrics} instance.
     */
    @Nonnull
    public JobMetrics filter(@Nonnull Predicate<Measurement> predicate) {
        Objects.requireNonNull(predicate, "predicate");

        Map<String, List<Measurement>> filteredMetrics = metrics.values().stream()
                .flatMap(Collection::stream)
                .filter(predicate)
                .collect(COLLECTOR);
        return new JobMetrics(filteredMetrics);
    }

    /**
     * Merges the current instance of {@link JobMetrics} with the provided
     * one and returns the result as a new {@link JobMetrics} object. The
     * returned object will contain all metric names from both sources and
     * a union of all their
     * {@link Measurement}s.
     */
    @Nonnull
    public JobMetrics merge(@Nonnull JobMetrics that) {
        Objects.requireNonNull(that, "that");
        Stream<Measurement> thisMeasurements = this.metrics.values().stream().flatMap(Collection::stream);
        Stream<Measurement> thatMeasurements = that.metrics.values().stream().flatMap(Collection::stream);
        return new JobMetrics(Stream.concat(thisMeasurements, thatMeasurements).collect(COLLECTOR));
    }

    /**
     * Prints out a multi-line, user friendly version of the content.
     */
    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, List<Measurement>> entry : metrics.entrySet()) {
            sb.append("\n").append(entry.getKey());

            List<Measurement> measurements = entry.getValue();
            for (Measurement measurement : measurements) {
                sb.append("\n\t").append(measurement);
            }
        }
        return sb.toString();
    }

    @Override
    public int getFactoryId() {
        return MetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
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

        return Objects.equals(metrics, ((JobMetrics) obj).metrics);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        metrics.entrySet().stream()
                .sorted(Comparator.comparing(Entry::getKey))
                .forEach(mainEntry -> {
                    sb.append(mainEntry.getKey()).append(":\n");
                    mainEntry.getValue().stream()
                            .collect(
                                    groupingBy(m -> {
                                                String vertex = m.getTag(MetricTags.VERTEX);
                                                return vertex == null ? "" : vertex;
                                            }
                                    )
                            )
                            .entrySet().stream()
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .forEach(
                                    e -> {
                                        String vertexName = e.getKey();
                                        sb.append("  ").append(vertexName).append(":\n");
                                        e.getValue().forEach(m -> sb.append("    ").append(m).append("\n"));
                                    }
                            );
                });
        return sb.toString();
    }
}
