/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertOrdered;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemoryManagementTest extends SimpleTestInClusterSupport {
    private static final int MAX_PROCESSOR_ACCUMULATED_RECORDS = 5;

    @Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return asList(
                test("Aggregation", false, (pipeline, itemCount) ->
                    pipeline.readFrom(TestSources.items(list(itemCount)))
                            .aggregate(AggregateOperations.toList())
                            .writeTo(assertOrdered(singletonList(list(itemCount))))
                ),
                test("Grouping", (pipeline, itemCount) ->
                    pipeline.readFrom(TestSources.items(list(itemCount)))
                            .groupingKey(wholeItem())
                            .aggregate(counting())
                            .writeTo(assertOrdered(cardinalities(itemCount)))
                ),
                test("Sorting", (pipeline, itemCount) ->
                    pipeline.readFrom(TestSources.items(list(itemCount)))
                            .sort()
                            .writeTo(assertOrdered(list(itemCount)))
                ),
                test("Joining", (pipeline, itemCount) -> {
                    BatchStage<Integer> left = pipeline
                            .readFrom(TestSources.items(list(itemCount)));
                    BatchStage<Entry<Integer, Long>> right = pipeline
                            .readFrom(TestSources.items(cardinalities(itemCount)));
                    left.hashJoin(right, JoinClause.joinMapEntries(wholeItem()), Util::entry)
                            .writeTo(assertOrdered(cardinalities(itemCount)));
                }),
                test("Transformation", (pipeline, itemCount) ->
                    pipeline.readFrom(TestSources.items(list(itemCount)))
                            .groupingKey(FunctionEx.identity())
                            .mapStateful(() -> 1, (a, s, i) -> i)
                            .writeTo(assertOrdered(list(itemCount)))
                ),
                test("Distinct", (pipeline, itemCount) ->
                    pipeline.readFrom(TestSources.items(list(itemCount)))
                            .distinct()
                            .writeTo(assertOrdered(list(itemCount)))
                )
        );
    }

    static Object[] test(String name, BiConsumer<Pipeline, Integer> configureFn) {
        return test(name, true, configureFn);
    }

    static Object[] test(String name, boolean expectOOME, BiConsumer<Pipeline, Integer> configureFn) {
        return new Object[] {name, expectOOME, configureFn};
    }

    @Parameter(0)
    public String test;

    @Parameter(1)
    public boolean expectOOME;

    @Parameter(2)
    public BiConsumer<Pipeline, Integer> configureFn;

    @BeforeClass
    public static void setUp() {
        Config config = smallInstanceConfig();
        config.getJetConfig()
                .setCooperativeThreadCount(1)
                .setMaxProcessorAccumulatedRecords(MAX_PROCESSOR_ACCUMULATED_RECORDS);

        initialize(1, config);
    }

    @Test
    public void testSuspendOnOOME_then_succeedAfterIncreasingLimit() {
        int itemCount = MAX_PROCESSOR_ACCUMULATED_RECORDS + 1;
        Pipeline pipeline = Pipeline.create();
        configureFn.accept(pipeline, itemCount);

        Job job = instance().getJet().newJob(pipeline, new JobConfig().setSuspendOnFailure(true));
        if (expectOOME) {
            assertJobSuspendedEventually(job);
            assertThat(job.getSuspensionCause().errorCause())
                    .contains("Exception thrown to prevent an OutOfMemoryError on this Hazelcast instance");
            job.updateConfig(new DeltaJobConfig().setMaxProcessorAccumulatedRecords(itemCount));
            job.resume();
        }
        job.join();
    }

    private static Collection<Integer> list(int numberOfItems) {
        return IntStream.range(0, numberOfItems).boxed().collect(toList());
    }

    private static Collection<Entry<Integer, Long>> cardinalities(int numberOfItems) {
        return IntStream.range(0, numberOfItems).mapToObj(i -> entry(i, 1L)).collect(toList());
    }
}
