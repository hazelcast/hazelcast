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

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.managementcenter.MetricsCompressor;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobMetricsUtil;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.jet.impl.JobMetricsUtil.addPrefixToDescriptor;

/**
 * A publisher which updates the latest metric values in {@link
 * JobExecutionService}.
 */
public class JobMetricsPublisher implements MetricsPublisher {

    private final JobExecutionService jobExecutionService;
    private final String namePrefix;
    private final Map<Long, MetricsCompressor> executionIdToCompressor = new HashMap<>();

    public JobMetricsPublisher(
            @Nonnull JobExecutionService jobExecutionService,
            @Nonnull Member member
    ) {
        Objects.requireNonNull(jobExecutionService, "jobExecutionService");
        Objects.requireNonNull(member, "member");

        this.jobExecutionService = jobExecutionService;
        this.namePrefix = JobMetricsUtil.getMemberPrefix(member);
    }

    @Override
    public void publishLong(String name, long value, Set<MetricTarget> excludedTargets) {
        MetricsCompressor metricsCompressor = getCompressor(name);
        if (metricsCompressor != null) {
            metricsCompressor.addLong(addPrefixToDescriptor(name, namePrefix), value);
        }
    }

    @Override
    public void publishDouble(String name, double value, Set<MetricTarget> excludedTargets) {
        MetricsCompressor metricsCompressor = getCompressor(name);
        if (metricsCompressor != null) {
            metricsCompressor.addDouble(addPrefixToDescriptor(name, namePrefix), value);
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

    private MetricsCompressor getCompressor(String name) {
        Long executionId = JobMetricsUtil.getExecutionIdFromMetricDescriptor(name);
        return executionId == null ? null :
                executionIdToCompressor.computeIfAbsent(executionId, id -> new MetricsCompressor());
    }
}
