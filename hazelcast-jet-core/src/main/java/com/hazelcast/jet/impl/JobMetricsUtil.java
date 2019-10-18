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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricsUtil;
import com.hazelcast.internal.metrics.managementcenter.Metric;
import com.hazelcast.internal.metrics.managementcenter.MetricConsumer;
import com.hazelcast.internal.metrics.managementcenter.MetricsCompressor;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class JobMetricsUtil {

    private static final Pattern METRIC_KEY_EXEC_ID_PATTERN =
            Pattern.compile("\\[module=jet,job=[^,]+,exec=([^,]+),.*");

    private JobMetricsUtil() {
    }

    public static Long getExecutionIdFromMetricDescriptor(@Nonnull String descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");

        Matcher m = METRIC_KEY_EXEC_ID_PATTERN.matcher(descriptor);
        if (!m.matches()) {
            // not a job-related metric, ignore it
            return null;
        }
        return Util.idFromString(m.group(1));
    }

    public static String addPrefixToDescriptor(@Nonnull String descriptor, @Nonnull String prefix) {
        assert !prefix.isEmpty() : "Prefix is empty";
        assert prefix.endsWith(",") : "Prefix should end in a comma";

        assert descriptor.length() >= 3 : "Descriptor too short";
        assert descriptor.startsWith("[") : "Descriptor of non-standard format";
        assert descriptor.endsWith("]") : "Descriptor of non-standard format";

        return "[" + prefix + descriptor.substring(1);
    }

    public static String getMemberPrefix(@Nonnull Member member) {
        Objects.requireNonNull(member, "member");

        String uuid = member.getUuid().toString();
        String address = member.getAddress().toString();
        return MetricTags.MEMBER + "=" + MetricsUtil.escapeMetricNamePart(uuid) + "," +
                MetricTags.ADDRESS + "=" + MetricsUtil.escapeMetricNamePart(address) + ",";
    }

    static JobMetrics toJobMetrics(List<RawJobMetrics> rawJobMetrics) {
        MetricKeyValueConsumer consumer = new MetricKeyValueConsumer();
        return JobMetrics.of(rawJobMetrics.stream()
                                          .filter(r -> r.getBlob() != null)
                                          .flatMap(r -> metricStream(r).map(metric ->
                                                  toMeasurement(r.getTimestamp(), metric, consumer)))
        );
    }

    private static Stream<Metric> metricStream(RawJobMetrics r) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        MetricsCompressor.decompressingIterator(r.getBlob()),
                        Spliterator.NONNULL
                ), false
        );
    }

    private static Measurement toMeasurement(long timestamp, Metric metric, MetricKeyValueConsumer kvConsumer) {
        metric.provide(kvConsumer);
        String descriptor = kvConsumer.key;
        Map<String, String> tags = MetricsUtil.parseMetricName(descriptor).stream()
                                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return Measurement.of(kvConsumer.value, timestamp, tags);
    }

    private static class MetricKeyValueConsumer implements MetricConsumer {

        String key;
        long value;

        @Override
        public void consumeLong(String key, long value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void consumeDouble(String key, double value) {
            this.key = key;
            this.value = (long) value;
        }
    }


}
