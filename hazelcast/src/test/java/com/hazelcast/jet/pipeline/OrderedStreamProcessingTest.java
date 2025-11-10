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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.GeneratorFunction;
import com.hazelcast.jet.pipeline.test.ParallelStreamP;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.LongStream;

import static com.hazelcast.function.Functions.wholeItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static java.util.stream.Collectors.toList;


@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class OrderedStreamProcessingTest extends JetTestSupport implements Serializable {

    // Used to set the LP of the stage with the higher value than upstream parallelism
    private static final int HIGH_LOCAL_PARALLELISM = 11;
    // Used to set the LP of the stage with the smaller value than upstream parallelism
    private static final int LOW_LOCAL_PARALLELISM = 2;
    private static final int ITEM_COUNT = 250;

    private static Pipeline p;
    private static JetInstance jet;

    @Parameter(value = 0)
    public FunctionEx<StreamStage<Long>, StreamStage<Long>> transform;

    @Parameter(value = 1)
    public String transformName;

    @BeforeClass
    public static void setupClass() {
        jet = (JetInstance) Hazelcast.newHazelcastInstance().getJet();
    }

    @Before
    public void setup() {
        p = Pipeline.create().setPreserveOrder(true);
    }

    @AfterClass
    public static void cleanup() {
        jet.shutdown();
    }

    @Parameters(name = "{index}: transform={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-high"
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(Traversers::singleton)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "flat-map-high"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-using-replicated-map-high"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-stateful-global-high"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-stateful-global-high"
                ),
                createParamSet(
                        stage -> stage
                                .<Long>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "custom-transform-high"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-using-imap-high"
                ),
                createParamSet(
                        stage -> stage
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "filter-high"
                ),
                createParamSet(
                        stage -> stage
                                .map(x -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-low"
                ),
                createParamSet(
                        stage -> stage
                                .flatMap(Traversers::singleton)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "flat-map-low"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-using-replicated-map-low"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-stateful-global-low"
                ),
                createParamSet(
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-stateful-global-low"
                ),
                createParamSet(
                        stage -> stage
                                .<Long>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "custom-transform-low"
                ),
                createParamSet(
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-using-imap-low"
                ),
                createParamSet(
                        stage -> stage
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "filter-low"
                ),
                createParamSet(
                        stage -> stage
                                .map(FunctionEx.identity())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-high-filter-low"
                ), createParamSet(
                        stage -> stage
                                .map(FunctionEx.identity())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM)
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-low-filter-high"
                )
        );
    }

    private static Object[] createParamSet(
            FunctionEx<StreamStage<Long>, StreamStage<Long>> transform,
            String transformName
    ) {
        return new Object[]{transform, transformName};
    }

    @Test
    public void when_source_is_non_partitioned() {
        int validatedItemCount = ITEM_COUNT;
        int itemsPerSecond = 5 * ITEM_COUNT;

        List<Long> sequence = LongStream.range(0, validatedItemCount).boxed().collect(toList());

        StreamStage<Long> srcStage = p.readFrom(TestSources.itemStream(itemsPerSecond, (ts, seq) -> seq))
                .withIngestionTimestamps();

        StreamStage<Long> applied = srcStage.apply(transform);

        applied.writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence.toArray())));

        Job job = jet.newJob(p);
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
    public void when_source_is_parallel() {
        int validatedItemCountPerGenerator = ITEM_COUNT;
        int eventsPerSecondPerGenerator = 5 * ITEM_COUNT;
        int generatorCount = 4;

        // Generate monotonic increasing items that are distinct for each generator.
        GeneratorFunction<Long> generator1 = (ts, seq) -> generatorCount * seq;
        GeneratorFunction<Long> generator2 = (ts, seq) -> generatorCount * seq + 1;
        GeneratorFunction<Long> generator3 = (ts, seq) -> generatorCount * seq + 2;
        GeneratorFunction<Long> generator4 = (ts, seq) -> generatorCount * seq + 3;

        StreamStage<Long> srcStage = p.readFrom(itemsParallel(
                eventsPerSecondPerGenerator,
                Arrays.asList(generator1, generator2, generator3, generator4))
        ).withIngestionTimestamps();

        StreamStage<Long> applied = srcStage.apply(transform);

        applied.mapStateful(() -> create(generatorCount), this::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", validatedItemCountPerGenerator <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));

        Job job = jet.newJob(p);
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
    public void when_source_is_parallel_2() {
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
                Arrays.asList(generator1, generator2, generator3, generator4))
        ).withIngestionTimestamps();

        StreamStage<Long> applied = srcStage.apply(transform);

        applied.filter(i -> i % generatorCount == 0)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence1.toArray())));

        applied.filter(i -> i % generatorCount == 1)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence2.toArray())));
        applied.filter(i -> i % generatorCount == 2)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence3.toArray())));
        applied.filter(i -> i % generatorCount == 3)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> Assert.assertArrayEquals(list.toArray(), sequence4.toArray())));

        Job job = jet.newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                    + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    /**
     * Returns a streaming source that generates events created by the {@code
     * generatorFn}s. It emits the data from different generators in parallel,
     * but preserves the order of every individual generator.
     * <p>
     * By default, it sets its preferred local parallelism equal to the number
     * of generator functions, thus having one processor for each of them even
     * on a single-member cluster. If there are more members, some
     * processors will remain idle.
     * <p>
     * The source supports {@linkplain
     * StreamSourceStage#withNativeTimestamps(long) native timestamps}. The
     * timestamp is the current system time at the moment the event is
     * generated. The source is not fault-tolerant. If the job restarts, all
     * the generator functions are reset and emit everything from the start.
     *
     */
    private static <T> StreamSource<T> itemsParallel(
            long eventsPerSecondPerGenerator, @Nonnull List<? extends GeneratorFunction<T>> generatorFns
    ) {
        Objects.requireNonNull(generatorFns, "generatorFns");

        return Sources.streamFromProcessorWithWatermarks("itemsParallel",
                true,
                eventTimePolicy -> ProcessorMetaSupplier.of(generatorFns.size(), () ->
                        new ParallelStreamP<>(eventsPerSecondPerGenerator, eventTimePolicy, generatorFns)));
    }

    private LongAccumulator[] create(int generatorCount) {
        LongAccumulator[] state = new LongAccumulator[generatorCount];
        for (int i = 0; i < generatorCount; i++) {
            state[i] = new LongAccumulator(Long.MIN_VALUE);
        }
        return state;
    }

    private boolean orderValidator(LongAccumulator[] s, Long eventValue) {
        LongAccumulator acc = s[eventValue.intValue() % s.length];
        if (acc.get() >= eventValue) {
            return false;
        } else {
            acc.set(eventValue);
            return true;
        }
    }
}
