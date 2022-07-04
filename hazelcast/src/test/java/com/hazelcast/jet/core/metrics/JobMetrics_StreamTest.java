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
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.metrics.JobMetrics_BatchTest.JOB_CONFIG_WITH_METRICS;
import static com.hazelcast.jet.core.metrics.MetricNames.EMITTED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobMetrics_StreamTest extends TestInClusterSupport {

    private static final String NOT_FILTER_OUT_PREFIX = "ok";
    private static final String FILTER_OUT_PREFIX = "nok";

    private static final String FLAT_MAP_AND_FILTER_VERTEX = "fused(map, filter)";

    private static String journalMapName;
    private static String sinkListName;

    @Before
    public void before() {
        journalMapName = JOURNALED_MAP_PREFIX + randomString();
        sinkListName = "sinkList" + randomString();
    }

    @Test
    public void when_jobRunning_then_metricsEventuallyExist() {
        Map<String, String> map = hz().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = hz().getList(sinkListName);

        // When
        Job job = hz().getJet().newJob(createPipeline());

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(3, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));
    }

    @Test
    public void when_suspendAndResume_then_metricsReset() {
        Map<String, String> map = hz().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = hz().getList(sinkListName);

        JobConfig jobConfig = new JobConfig()
            .setStoreMetricsAfterJobCompletion(true)
            .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        // When
        Job job = hz().getJet().newJob(createPipeline(), jobConfig);

        putIntoMap(map, 1, 1);

        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        job.suspend();

        assertJobStatusEventually(job, SUSPENDED);
        assertTrue(job.getMetrics().metrics().isEmpty());

        putIntoMap(map, 1, 1);
        job.resume();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(4, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 2, 1));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(5, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 4, 2));
    }

    @Test
    public void when_jobRestarted_then_metricsReset() {
        Map<String, String> map = hz().getMap(journalMapName);
        putIntoMap(map, 2, 1);
        List<String> sink = hz().getList(sinkListName);

        Job job = hz().getJet().newJob(createPipeline(), JOB_CONFIG_WITH_METRICS);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);

        assertTrueEventually(() -> assertEquals(3, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        // When
        job.restart();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(6, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(7, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 7, 3));
    }

    @Test
    public void when_jobRestarted_then_metricsReset_withJournal() {
        Map<String, String> map = hz().getMap(journalMapName);
        List<String> sink = hz().getList(sinkListName);

        Job job = hz().getJet().newJob(createPipeline(), JOB_CONFIG_WITH_METRICS);

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 0, 0));

        putIntoMap(map, 2, 1);

        assertTrueEventually(() -> assertEquals(2, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        // When
        job.restart();

        assertJobStatusEventually(job, RUNNING);
        assertTrueEventually(() -> assertEquals(4, sink.size()));
        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 3, 1));

        putIntoMap(map, 1, 1);
        assertTrueEventually(() -> assertEquals(5, sink.size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics(), 5, 2));
    }

    private Pipeline createPipeline() {
        Pipeline p = Pipeline.create();
        p.<Map.Entry<String, String>>readFrom(Sources.mapJournal(journalMapName, JournalInitialPosition.START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(Map.Entry::getKey)
                .filter(word -> !word.startsWith(FILTER_OUT_PREFIX))
                .writeTo(Sinks.list(sinkListName));
        return p;
    }

    private void putIntoMap(Map<String, String> map, int notFilterOutItemsCount, int filterOutItemsCount) {
        for (int i = 0; i < notFilterOutItemsCount; i++) {
            map.put(NOT_FILTER_OUT_PREFIX + randomString(), "whateverHere");
        }
        for (int i = 0; i < filterOutItemsCount; i++) {
            map.put(FILTER_OUT_PREFIX + randomString(), "whateverHere");
        }
    }

    private void assertMetrics(JobMetrics metrics, int allItems, int filterOutItems) {
        Assert.assertNotNull(metrics);
        assertEquals(allItems, sumValueFor(metrics, "mapJournalSource(" + journalMapName + ")", EMITTED_COUNT));
        assertEquals(allItems, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, RECEIVED_COUNT));
        assertEquals(allItems - filterOutItems, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, EMITTED_COUNT));
        assertEquals(allItems - filterOutItems,
                sumValueFor(metrics, "listSink(" + sinkListName + ")", RECEIVED_COUNT));
    }

    private long sumValueFor(JobMetrics metrics, String vertex, String metric) {
        Collection<Measurement> measurements = metrics
                .filter(MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, vertex)
                            .and(MeasurementPredicates.tagValueEquals(MetricTags.ORDINAL, "snapshot").negate()))
                .get(metric);
        return measurements.stream().mapToLong(Measurement::value).sum();
    }
}
