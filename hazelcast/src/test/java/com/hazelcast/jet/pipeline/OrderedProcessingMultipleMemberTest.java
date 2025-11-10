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

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.LongStream;

import static com.hazelcast.function.Functions.wholeItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 *  The tests of this class run on the same Jet cluster. In order for
 *  the tests to run quickly, we do not start a new cluster after each
 *  test. But if some test spoils the cluster, it may also cause other
 *  tests to fail. But since we did not expect this case, we preferred
 *  performance in this tradeoff. Isolation between tests is provided
 *  by using different mapJournals in each test.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class OrderedProcessingMultipleMemberTest extends SimpleTestInClusterSupport {

    // Used to set the LP of the stage with the higher value than upstream parallelism
    private static final int HIGH_LOCAL_PARALLELISM = 11;
    // Used to set the LP of the stage with the smaller value than upstream parallelism
    private static final int LOW_LOCAL_PARALLELISM = 2;
    private static final int INSTANCE_COUNT = 2;
    private static final String JOURNALED_MAP_PREFIX = "test-map-";

    private static Pipeline p;

    @Parameter(value = 0)
    public int idx;

    @Parameter(value = 1)
    public FunctionEx<StreamStage<Map.Entry<Long, Long>>, StreamStage<Map.Entry<Long, Long>>> transform;

    @Parameter(value = 2)
    public String transformName;

    @BeforeClass
    public static void setupClass() {
        Config config = smallInstanceConfig();
        EventJournalConfig eventJournalConfig = config
                .getMapConfig(JOURNALED_MAP_PREFIX + '*')
                .getEventJournalConfig();
        eventJournalConfig.setEnabled(true);
        eventJournalConfig.setCapacity(30000); // 30000/271 ~= 111 item per partition

        initialize(INSTANCE_COUNT, config);
    }

    @Before
    public void setup() {
        p = Pipeline.create().setPreserveOrder(true);
    }

    @Parameters(name = "{index}: transform={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                createParamSet(
                        0,
                        stage -> stage
                                .map(FunctionEx.identity())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-high"
                ),
                createParamSet(
                        1,
                        stage -> stage
                                .flatMap(Traversers::singleton)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "flat-map-high"
                ),
                createParamSet(
                        2,
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-using-imap-high"
                ),
                createParamSet(
                        3,
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-using-replicated-map-high"
                ),
                createParamSet(
                        4,
                        stage -> stage
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "filter-high"
                ),
                createParamSet(
                        5,
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "map-stateful-global-high"
                ),
                createParamSet(
                        6,
                        stage -> stage
                                .<Map.Entry<Long, Long>>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM),
                        "custom-transform-high"
                ),
                createParamSet(
                        7,
                        stage -> stage
                                .map(FunctionEx.identity())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-low"
                ),
                createParamSet(
                        8,
                        stage -> stage
                                .flatMap(Traversers::singleton)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "flat-map-low"
                ),
                createParamSet(
                        9,
                        stage -> stage
                                .mapUsingIMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-using-imap-low"
                ),
                createParamSet(
                        10,
                        stage -> stage
                                .mapUsingReplicatedMap("test-map", wholeItem(), (x, ignored) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-using-replicated-map-low"
                ),
                createParamSet(
                        11,
                        stage -> stage
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "filter-low"
                ),
                createParamSet(
                        12,
                        stage -> stage
                                .mapStateful(LongAccumulator::new, (s, x) -> x)
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-stateful-global-low"
                ),
                createParamSet(
                        13,
                        stage -> stage
                                .<Map.Entry<Long, Long>>customTransform("custom-transform",
                                        Processors.mapP(FunctionEx.identity()))
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "custom-transform-low"
                ),
                createParamSet(
                        14,
                        stage -> stage
                                .map(FunctionEx.identity())
                                .setLocalParallelism(HIGH_LOCAL_PARALLELISM)
                                .filter(PredicateEx.alwaysTrue())
                                .setLocalParallelism(LOW_LOCAL_PARALLELISM),
                        "map-high-filter-low"
                ), createParamSet(
                        15,
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
            int idx,
            FunctionEx<StreamStage<Map.Entry<Long, Long>>, StreamStage<Map.Entry<Long, Long>>> transform,
            String transformName
    ) {
        return new Object[]{idx, transform, transformName};
    }

    @Test
    public void multiple_nodes() {
        int itemCount = 250; // because of mapJournal's capacity, increasing this value too much can break the test
        int keyCount = 8;

        String mapName = "test-map-" + idx;
        StreamStage<Map.Entry<Long, Long>> srcStage = p.readFrom(
                Sources.<Long, Long>mapJournal(mapName, JournalInitialPosition.START_FROM_OLDEST)
        ).withoutTimestamps();
        StreamStage<Map.Entry<Long, Long>> applied = srcStage.apply(transform);

        applied.groupingKey(Map.Entry::getKey)
                .mapStateful(() -> create(keyCount), OrderedProcessingMultipleMemberTest::orderValidator)
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> {
                            assertTrue("when", itemCount <= list.size());
                            assertFalse("There is some reordered items in the list", list.contains(false));
                        }
                ));

        IMap<Long, Long> testMap = instance().getMap(mapName);
        LongStream.range(0, itemCount)
                .boxed()
                .forEachOrdered(i -> testMap.put(i % keyCount, i));

        Job job = instance().getJet().newJob(p);
        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            testMap.clear();
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

    private static boolean orderValidator(LongAccumulator[] s, Long key, Map.Entry<Long, Long> entry) {
        LongAccumulator acc = s[key.intValue()];
        long value = entry.getValue();
        if (acc.get() >= value) {
            return false;
        } else {
            acc.set(value);
            return true;
        }
    }
}
