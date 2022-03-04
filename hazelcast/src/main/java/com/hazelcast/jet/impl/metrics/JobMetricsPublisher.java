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

package com.hazelcast.jet.impl.metrics;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobMetricsUtil;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A publisher which updates the latest metric values in {@link
 * JobExecutionService}.
 */
public class JobMetricsPublisher implements MetricsPublisher {

    private final JobExecutionService jobExecutionService;
    private final UnaryOperator<MetricDescriptor> namePrefixFn;
    private final Map<Long, MetricsCompressor> executionIdToCompressor = new HashMap<>();

    public JobMetricsPublisher(
            @Nonnull JobExecutionService jobExecutionService,
            @Nonnull Member member
    ) {
        Objects.requireNonNull(jobExecutionService, "jobExecutionService");
        Objects.requireNonNull(member, "member");

        this.jobExecutionService = jobExecutionService;
        this.namePrefixFn = JobMetricsUtil.addMemberPrefixFn(member);
    }

    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        MetricsCompressor metricsCompressor = getCompressor(descriptor);
        if (metricsCompressor != null) {
            metricsCompressor.addLong(namePrefixFn.apply(descriptor), value);
        }
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        MetricsCompressor metricsCompressor = getCompressor(descriptor);
        if (metricsCompressor != null) {
            metricsCompressor.addDouble(namePrefixFn.apply(descriptor), value);
        }
    }

    @Override
    public void whenComplete() {
        Set<Map.Entry<Long, MetricsCompressor>> compressorEntries = executionIdToCompressor.entrySet();
        for (Iterator<Map.Entry<Long, MetricsCompressor>> it = compressorEntries.iterator(); it.hasNext();) {
            Map.Entry<Long, MetricsCompressor> entry = it.next();

            MetricsCompressor compressor = entry.getValue();
            // remove compressors that didn't receive any metrics
            if (compressor.count() == 0) {
                it.remove();
            }

            Long executionId = entry.getKey();
            byte[] blob = compressor.getBlobAndReset();
            jobExecutionService.updateMetrics(executionId, RawJobMetrics.of(blob));
        }
    }

    @Override
    public String name() {
        return "Job Metrics Publisher";
    }

    private MetricsCompressor getCompressor(MetricDescriptor descriptor) {
        Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
        return executionId == null ?
                null
                : executionIdToCompressor.computeIfAbsent(executionId, id -> new MetricsCompressor());
    }
}
