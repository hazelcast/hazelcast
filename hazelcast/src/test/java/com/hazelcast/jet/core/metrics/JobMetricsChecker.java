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

package com.hazelcast.jet.core.metrics;

import com.hazelcast.jet.Job;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

final class JobMetricsChecker {

    private final Job job;
    private final Predicate<Measurement> filter;

    JobMetricsChecker(Job job) {
        this(job, alwaysTrue());
    }

    JobMetricsChecker(Job job, Predicate<Measurement> filter) {
        this.job = job;
        this.filter = filter;
    }

    void assertNoMetricValues(String metricName) {
        JobMetrics jobMetrics = getJobMetrics();
        List<Measurement> measurements = jobMetrics.get(metricName);
        assertTrue(
                String.format("Did not expect measurements for metric '%s', but there were some", metricName),
                measurements.isEmpty()
        );
    }

    void assertSummedMetricValue(String metricName, long expectedValue) {
        assertAggregatedMetricValue(metricName, expectedValue,
                m -> m.stream().mapToLong(Measurement::value).sum());
    }

    void assertRandomMetricValue(String metricName, long expectedValue) {
        assertAggregatedMetricValue(metricName, expectedValue,
                m -> m.stream().limit(1).mapToLong(Measurement::value).sum());
    }

    long assertSummedMetricValueAtLeast(String metricName, long minExpectedValue) {
        return assertAggregatedMetricValueAtLeast(metricName, minExpectedValue,
                m -> m.stream().mapToLong(Measurement::value).sum());
    }

    long assertRandomMetricValueAtLeast(String metricName, long minExpectedValue) {
        return assertAggregatedMetricValueAtLeast(metricName, minExpectedValue,
                m -> m.stream().limit(1).mapToLong(Measurement::value).sum());
    }

    private void assertAggregatedMetricValue(String metricName, long expectedValue,
                                             Function<List<Measurement>, Long> aggregateFn) {
        List<Measurement> measurements = getJobMetrics().get(metricName);
        assertFalse(
                String.format("Expected measurements for metric '%s', but there were none", metricName),
                measurements.isEmpty()
        );
        long actualValue = aggregateFn.apply(measurements);
        assertEquals(
                String.format("Expected %d for metric '%s', but got %d instead", expectedValue, metricName,
                        actualValue),
                expectedValue,
                actualValue
        );
    }

    private long assertAggregatedMetricValueAtLeast(String metricName, long minExpectedValue,
                                                    Function<List<Measurement>, Long> aggregateFn) {
        List<Measurement> measurements = getJobMetrics().get(metricName);
        assertFalse(
                String.format("Expected measurements for metric '%s', but there were none", metricName),
                measurements.isEmpty()
        );
        long actualValue = aggregateFn.apply(measurements);
        assertTrue(
                String.format("Expected a value of at least %d for metric '%s', but got %d",
                        minExpectedValue, metricName, actualValue),
                minExpectedValue <= actualValue
        );
        return actualValue;
    }

    private JobMetrics getJobMetrics() {
        return job.getMetrics().filter(filter);
    }
}
