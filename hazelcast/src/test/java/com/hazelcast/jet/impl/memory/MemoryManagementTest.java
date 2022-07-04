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

package com.hazelcast.jet.impl.memory;

import com.hazelcast.config.Config;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.Sinks.noop;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MemoryManagementTest extends SimpleTestInClusterSupport {

    private static final int MAX_PROCESSOR_ACCUMULATED_RECORDS = 5;

    @BeforeClass
    public static void setUpClass() {
        Config config = smallInstanceConfig();
        config.getJetConfig()
                .setCooperativeThreadCount(1)
                .setMaxProcessorAccumulatedRecords(MAX_PROCESSOR_ACCUMULATED_RECORDS);

        initialize(1, config);
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileAggregating_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .aggregate(AggregateOperations.toList())
                .writeTo(assertOrdered(singletonList(list(MAX_PROCESSOR_ACCUMULATED_RECORDS))));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileGrouping_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .groupingKey(wholeItem())
                .aggregate(counting())
                .writeTo(assertOrdered(cardinalities(MAX_PROCESSOR_ACCUMULATED_RECORDS)));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileGrouping_then_throws() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)))
                .groupingKey(wholeItem())
                .aggregate(counting())
                .writeTo(noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileSorting_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .sort()
                .writeTo(assertOrdered(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileSorting_then_throws() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)))
                .sort()
                .writeTo(noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileJoining_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Integer> left = pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)));
        BatchStage<Entry<Integer, Long>> right = pipeline.readFrom(TestSources.items(cardinalities(MAX_PROCESSOR_ACCUMULATED_RECORDS)));
        left.hashJoin(right, JoinClause.joinMapEntries(wholeItem()), Util::entry)
                .writeTo(assertOrdered(cardinalities(MAX_PROCESSOR_ACCUMULATED_RECORDS)));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileJoining_then_throws() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Integer> left = pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)));
        BatchStage<Entry<Integer, Long>> right = pipeline.readFrom(TestSources.items(cardinalities(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)));
        left.hashJoin(right, JoinClause.joinMapEntries(wholeItem()), Util::entry)
                .writeTo(noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileTransforming_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .mapStateful(() -> 1, (s, i) -> i)
                .writeTo(assertOrdered(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileTransforming_then_throws() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)))
                .groupingKey(FunctionEx.identity())
                .mapStateful(() -> 1, (a, k, i) -> i)
                .writeTo(noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsNotExceededWhileDistinct_then_succeeds() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .distinct()
                .writeTo(assertOrdered(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)));

        instance().getJet().newJob(pipeline).join();
    }

    @Test
    public void when_maxAccumulatedRecordsCountIsExceededWhileDistinct_then_throws() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS + 1)))
                .distinct()
                .writeTo(noop());

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    @Test
    public void test_jobConfigurationHasPrecedenceOverInstanceOne() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(list(MAX_PROCESSOR_ACCUMULATED_RECORDS)))
                .groupingKey(wholeItem())
                .aggregate(counting())
                .writeTo(noop());

        JobConfig jobConfig = new JobConfig().setMaxProcessorAccumulatedRecords(MAX_PROCESSOR_ACCUMULATED_RECORDS - 1);

        assertThatThrownBy(() -> instance().getJet().newJob(pipeline, jobConfig).join())
                .hasMessageContaining("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
    }

    private static Collection<Integer> list(int numberOfItems) {
        return IntStream.range(0, numberOfItems).boxed().collect(toList());
    }

    @SuppressWarnings("SameParameterValue")
    private static Collection<Entry<Integer, Long>> cardinalities(int numberOfItems) {
        return IntStream.range(0, numberOfItems).mapToObj(i -> entry(i, 1L)).collect(toList());
    }
}
