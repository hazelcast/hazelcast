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
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.managementcenter.Metric;
import com.hazelcast.internal.metrics.managementcenter.MetricConsumer;
import com.hazelcast.internal.metrics.managementcenter.MetricsCompressor;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Util.idFromString;

public final class JobMetricsUtil {

    private JobMetricsUtil() {
    }

    public static Long getExecutionIdFromMetricsDescriptor(MetricDescriptor descriptor) {
        final String[] val = new String[1];
        descriptor.readTags((k, v) -> {
            if (MetricTags.EXECUTION.equals(k)) {
                val[0] = v;
            }
        });
        return idFromString(val[0]);
    }

    public static UnaryOperator<MetricDescriptor> addMemberPrefixFn(@Nonnull Member member) {
        String uuid = member.getUuid().toString();
        String addr = member.getAddress().toString();
        return d -> d.copy().withTag(MetricTags.MEMBER, uuid).withTag(MetricTags.ADDRESS, addr);
    }

    static JobMetrics toJobMetrics(List<RawJobMetrics> rawJobMetrics) {
        JobMetricsConsumer consumer = new JobMetricsConsumer();
        return JobMetrics.of(rawJobMetrics.stream()
              .filter(r -> r.getBlob() != null)
              .flatMap(r -> metricStream(r).map(m -> toMeasurement(r.getTimestamp(), m, consumer)))
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

    private static Measurement toMeasurement(long timestamp, Metric metric, JobMetricsConsumer kvConsumer) {
        metric.provide(kvConsumer);
        MetricDescriptor descriptor = kvConsumer.descriptor;
        Map<String, String> tags = MapUtil.createHashMap(descriptor.tagCount());
        descriptor.readTags(tags::put);
        return Measurement.of(kvConsumer.descriptor.metric(), kvConsumer.value, timestamp, tags);
    }

    private static class JobMetricsConsumer implements MetricConsumer {

        MetricDescriptor descriptor;
        long value;

        @Override
        public void consumeLong(MetricDescriptor descriptor, long value) {
            this.descriptor = descriptor;
            this.value = value;
        }

        @Override
        public void consumeDouble(MetricDescriptor descriptor, double value) {
            this.descriptor = descriptor;
            this.value = (long) value;
        }
    }


}
