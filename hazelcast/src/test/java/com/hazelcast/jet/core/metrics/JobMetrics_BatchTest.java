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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.metrics.MetricNames.EMITTED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_COUNT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobMetrics_BatchTest extends TestInClusterSupport {

    static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private static final String SOURCE_VERTEX = "items";
    private static final String FLAT_MAP_AND_FILTER_VERTEX = "fused(flat-map, filter)";
    private static final String GROUP_AND_AGGREGATE_PREPARE_VERTEX = "group-and-aggregate-prepare";
    private static final String GROUP_AND_AGGREGATE_VERTEX = "group-and-aggregate";
    private static final String SINK_VERTEX = "mapSink(counts)";

    private static final String COMMON_TEXT = "look at some common text here and uncommon text here";

    @Test
    public void when_jobCompleted_then_metricsExist() {
        Pipeline p = createPipeline();

        // When
        Job job = execute(p, JOB_CONFIG_WITH_METRICS);

        // Then
        assertMetrics(job.getMetrics());
    }

    @Test
    public void when_storeMetricsAfterJobCompletionDisabled_then_metricsEmpty() {
        Pipeline p = createPipeline();

        // When
        Job job = execute(p, new JobConfig());

        // Then
        assertEquals("non-empty metrics", JobMetrics.empty(), job.getMetrics());
    }

    @Test
    public void when_memberAddedAfterJobFinished_then_metricsNotAffected() {
        Pipeline p = createPipeline();

        Job job = execute(p, JOB_CONFIG_WITH_METRICS);

        // When
        HazelcastInstance instance = factory.newHazelcastInstance(prepareConfig());
        try {
            assertClusterSizeEventually(MEMBER_COUNT + 1, hz());
            // Then
            assertMetrics(job.getMetrics());
        } finally {
            instance.shutdown();
            assertClusterSizeEventually(MEMBER_COUNT, hz());
        }
    }

    @Test
    public void when_memberRemovedAfterJobFinished_then_metricsNotAffected() {
        Pipeline p = createPipeline();

        HazelcastInstance newMember = factory.newHazelcastInstance(prepareConfig());
        Job job;
        try {
            assertClusterSizeEventually(MEMBER_COUNT + 1, hz());
            job = execute(p, JOB_CONFIG_WITH_METRICS);
        } finally {
            newMember.shutdown();
        }
        assertClusterSizeEventually(MEMBER_COUNT, hz());
        assertMetrics(job.getMetrics());
    }

    @Test
    public void when_twoDifferentPipelines_then_haveDifferentMetrics() {
        String anotherText = "look at some common text here and here";
        Pipeline p = createPipeline();
        Pipeline p2 = createPipeline(anotherText);

        Job job = hz().getJet().newJob(p, JOB_CONFIG_WITH_METRICS);
        Job job2 = hz().getJet().newJob(p2, JOB_CONFIG_WITH_METRICS);
        job.join();
        job2.join();

        assertNotEquals(job.getMetrics(), job2.getMetrics());
        assertMetrics(job.getMetrics());
        assertMetrics(job2.getMetrics(), anotherText);
    }

    @Test
    public void when_twoDifferentJobsForTheSamePipeline_then_haveDifferentMetrics() {
        Pipeline p = createPipeline();

        Job job = hz().getJet().newJob(p, JOB_CONFIG_WITH_METRICS);
        Job job2 = hz().getJet().newJob(p, JOB_CONFIG_WITH_METRICS);
        job.join();
        job2.join();

        assertNotEquals(job.getMetrics(), job2.getMetrics());
        assertMetrics(job.getMetrics());
        assertMetrics(job2.getMetrics());
    }

    private Pipeline createPipeline() {
        return createPipeline(COMMON_TEXT);
    }

    private Pipeline createPipeline(String text) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(text))
         .flatMap(line -> traverseArray(line.toLowerCase(Locale.ROOT).split("\\W+")))
         .filter(word -> !word.isEmpty())
         .groupingKey(wholeItem())
         .aggregate(counting())
         .writeTo(Sinks.map("counts"));
        return p;
    }

    private void assertMetrics(JobMetrics metrics) {
        assertMetrics(metrics, COMMON_TEXT);
    }

    private void assertMetrics(JobMetrics metrics, String originalText) {
        Assert.assertNotNull(metrics);

        String[] words = originalText.split("\\W+");
        int wordCount = words.length;
        int uniqueWordCount = new HashSet<>(asList(words)).size();

        assertEquals(1, sumValueFor(metrics, SOURCE_VERTEX, EMITTED_COUNT));
        assertEquals(1, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, RECEIVED_COUNT));
        assertEquals(wordCount, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, EMITTED_COUNT));
        assertEquals(wordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_PREPARE_VERTEX, RECEIVED_COUNT));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_PREPARE_VERTEX, EMITTED_COUNT));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_VERTEX, RECEIVED_COUNT));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_VERTEX, EMITTED_COUNT));
        assertEquals(uniqueWordCount, sumValueFor(metrics, SINK_VERTEX, RECEIVED_COUNT));
    }

    private long sumValueFor(JobMetrics metrics, String vertex, String metric) {
        Collection<Measurement> measurements = metrics
                .filter(MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, vertex))
                .get(metric);
        return measurements.stream().mapToLong(Measurement::value).sum();
    }
}
