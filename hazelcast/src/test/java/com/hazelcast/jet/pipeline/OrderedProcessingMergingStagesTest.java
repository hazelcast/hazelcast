/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.jet.pipeline.test.ParallelStreamP;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.LongStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.util.stream.Collectors.toList;

@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class OrderedProcessingMergingStagesTest extends JetTestSupport implements Serializable {

    private static final int ITEM_COUNT = 250;
    // Used to set the LP of the stage with the higher value than upstream parallelism
    private static final int HIGH_LOCAL_PARALLELISM = 11;
    // Used to set the LP of the stage with the smaller value than upstream parallelism
    private static final int LOW_LOCAL_PARALLELISM = 2;
    private static Pipeline p;
    private static HazelcastInstance hz;


    @BeforeClass
    public static void setupClass() {
        hz = Hazelcast.newHazelcastInstance(smallInstanceConfig());
    }

    @Before
    public void setup() {
        p = Pipeline.create().setPreserveOrder(true);
    }

    @AfterClass
    public static void cleanup() {
        hz.shutdown();
    }


    @Test
    public void when_merge_applied_partial_orders_are_preserved() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 4;

        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Long> generator1 = (ts, seq) -> generatorCount * seq;
        GeneratorFunction<Long> generator2 = (ts, seq) -> generatorCount * seq + 1;
        GeneratorFunction<Long> generator3 = (ts, seq) -> generatorCount * seq + 2;
        GeneratorFunction<Long> generator4 = (ts, seq) -> generatorCount * seq + 3;

        List<Long> sequence1 = LongStream.range(0, validatedItemCountPerGenerator)
                .map(i -> generatorCount * i).boxed().collect(toList());
        List<Long> sequence2 = LongStream.range(0, validatedItemCountPerGenerator)
                .map(i -> generatorCount * i + 1).boxed().collect(toList());
        List<Long> sequence3 = LongStream.range(0, validatedItemCountPerGenerator)
                .map(i -> generatorCount * i + 2).boxed().collect(toList());
        List<Long> sequence4 = LongStream.range(0, validatedItemCountPerGenerator)
                .map(i -> generatorCount * i + 3).boxed().collect(toList());

        StreamStage<Long> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2))
        ).withIngestionTimestamps()
                .setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        StreamStage<Long> srcStage2 = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator3, generator4))
        ).withIngestionTimestamps()
                .setLocalParallelism(LOW_LOCAL_PARALLELISM);


        StreamStage<Long> merged = srcStage.merge(srcStage2).setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        merged.filter(i -> i % generatorCount == 0)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence1.toArray())));

        merged.filter(i -> i % generatorCount == 1)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence2.toArray())));
        merged.filter(i -> i % generatorCount == 2)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence3.toArray())));
        merged.filter(i -> i % generatorCount == 3)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence4.toArray())));

        Job job = hz.getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void when_hashJoin_applied_primary_stream_order_is_preserved() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 2;
        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Map.Entry<Long, Long>> generator1 = (ts, seq) -> Util.entry(0L, generatorCount * seq);
        GeneratorFunction<Map.Entry<Long, Long>> generator2 = (ts, seq) ->
                Util.entry(1L, generatorCount * seq + 1);

        StreamStage<Map.Entry<Long, Long>> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2))
        ).withIngestionTimestamps().setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        BatchStage<Map.Entry<Long, Long>> batchStage = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));

        StreamStage<Map.Entry<Long, Long>> joined = srcStage.hashJoin(batchStage,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                (primary, stage) -> primary).setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        joined.groupingKey(Map.Entry::getKey)
                .mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", validatedItemCountPerGenerator <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));
        Job job = hz.getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void when_hashJoin2_applied_primary_stream_order_is_preserved() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 2;
        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Map.Entry<Long, Long>> generator1 = (ts, seq) ->
                Util.entry(0L, generatorCount * seq);
        GeneratorFunction<Map.Entry<Long, Long>> generator2 = (ts, seq) ->
                Util.entry(1L, generatorCount * seq + 1);

        StreamStage<Map.Entry<Long, Long>> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2))
        ).withIngestionTimestamps();

        BatchStage<Map.Entry<Long, Long>> batchStage = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));

        BatchStage<Map.Entry<Long, Long>> batchStage2 = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));

        StreamStage<Map.Entry<Long, Long>> joined = srcStage.hashJoin2(batchStage,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                batchStage2,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                (primary, stg1, stg2) -> primary).setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        joined.groupingKey(Map.Entry::getKey)
                .mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", validatedItemCountPerGenerator <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));
        Job job = hz.getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void when_innerJoin_applied_primary_stream_order_is_preserved() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 2;
        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Map.Entry<Long, Long>> generator1 = (ts, seq) ->
                Util.entry(0L, generatorCount * seq);
        GeneratorFunction<Map.Entry<Long, Long>> generator2 = (ts, seq) ->
                Util.entry(1L, generatorCount * seq + 1);

        StreamStage<Map.Entry<Long, Long>> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2))
        ).withIngestionTimestamps().setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        BatchStage<Map.Entry<Long, Long>> batchStage = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));

        StreamStage<Map.Entry<Long, Long>> joined = srcStage.innerHashJoin(batchStage,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                (primary, stg) -> primary).setLocalParallelism(LOW_LOCAL_PARALLELISM);

        joined.groupingKey(Map.Entry::getKey)
                .mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", validatedItemCountPerGenerator <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));
        Job job = hz.getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void when_innerJoin2_applied_primary_stream_order_is_preserved() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 2;
        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Map.Entry<Long, Long>> generator1 = (ts, seq) ->
                Util.entry(0L, generatorCount * seq);
        GeneratorFunction<Map.Entry<Long, Long>> generator2 = (ts, seq) ->
                Util.entry(1L, generatorCount * seq + 1);

        StreamStage<Map.Entry<Long, Long>> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2))
        ).withIngestionTimestamps().setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        BatchStage<Map.Entry<Long, Long>> batchStage = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));
        BatchStage<Map.Entry<Long, Long>> batchStage2 = p.readFrom(TestSources.items(Util.entry(0L, 0L),
                Util.entry(1L, 0L)));

        StreamStage<Map.Entry<Long, Long>> joined = srcStage.innerHashJoin2(batchStage,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                batchStage2,
                JoinClause.onKeys(Map.Entry::getKey, Map.Entry::getKey),
                (primary, stg1, stg2) -> primary).setLocalParallelism(HIGH_LOCAL_PARALLELISM);

        joined.groupingKey(Map.Entry::getKey)
                .mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", validatedItemCountPerGenerator <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));
        Job job = hz.getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }


    private static LongAccumulator[] create(int keyCount) {
        LongAccumulator[] state = new LongAccumulator[keyCount];
        for (int i = 0; i < keyCount; i++) {
            state[i] = new LongAccumulator(Long.MIN_VALUE);
        }
        return state;
    }

    private boolean orderValidator(LongAccumulator[] s, Long key, Map.Entry<Long, Long> entry) {
        LongAccumulator acc = s[key.intValue()];
        long value = entry.getValue();
        if (acc.get() >= value) {
            return false;
        } else {
            acc.set(value);
            return true;
        }
    }

    private static <T> StreamSource<T> itemsParallel(
            long eventsPerSecondPerGenerator, @Nonnull List<? extends GeneratorFunction<T>> generatorFns
    ) {
        Objects.requireNonNull(generatorFns, "generatorFns");

        return Sources.streamFromProcessorWithWatermarks("itemsParallel",
                true,
                eventTimePolicy -> ProcessorMetaSupplier.of(generatorFns.size(), () ->
                        new ParallelStreamP<>(eventsPerSecondPerGenerator, eventTimePolicy, generatorFns)));
    }

}
