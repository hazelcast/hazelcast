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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class StatefulMappingWindowingTest extends JetTestSupport {

    private static final String SINK_NAME = StatefulMappingWindowingTest.class.getSimpleName() + "_sink";
    private static final int ITEMS_COUNT = 1000;
    private static final int WINDOWS_COUNT = 10;
    private static final int TS_MULTIPLY_FACTOR = 100;
    private static final int WINDOW_SIZE = TS_MULTIPLY_FACTOR * (ITEMS_COUNT / WINDOWS_COUNT);
    private static final long TTL = 1000;

    private static List<Long> counterValues;
    private static AtomicLong evictCount;
    private JetInstance instance;

    @Before
    public void setup() {
        counterValues = new ArrayList<>();
        evictCount = new AtomicLong();
        instance = createJetMembers(new JetConfig(), 2)[0];
    }

    @Test
    public void mapStateful_whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.mapStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return input;
                        }));
    }

    @Test
    public void flatMapStateful_whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.flatMapStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return Traversers.singleton(input);
                        }));
    }

    @Test
    public void filterStateful_whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.filterStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return true;
                        }));
    }

    @Test
    public void mapStateful_whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.mapStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return input;
                        }));
    }

    @Test
    public void flatMapStateful_whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.flatMapStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return Traversers.singleton(input);
                        }));
    }

    @Test
    public void filterStateful_whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized(generalStage
                -> generalStage.filterStateful(
                        AtomicLong::new,
                        (atomicLong, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return true;
                        }));
    }

    @Test
    public void mapStateful_whenWindowingAfterGeneralStageWithKey_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterGeneralStageWithKey_thenStateObjectIsNotReinitialized(generalStageWithKey
                -> generalStageWithKey.mapStateful(
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return input.sequence();
                        }));
    }

    @Test
    public void flatMapStateful_whenWindowingAfterGeneralStageWithKey_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterGeneralStageWithKey_thenStateObjectIsNotReinitialized(generalStageWithKey
                -> generalStageWithKey.flatMapStateful(
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return Traversers.singleton(input.sequence());
                        }));
    }

    @Test
    public void mapStateful_whenWindowingAfterStreamStageWithKey_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterStreamStageWithKey_thenStateObjectIsNotReinitialized(streamStageWithKey
                -> streamStageWithKey.mapStateful(
                        TTL,
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return input.sequence();
                        },
                        (atomicLong, key, wm) -> {
                            evictCount.incrementAndGet();
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_whenWindowingAfterStreamStageWithKey_thenStateObjectIsNotReinitialized() {
        whenWindowingAfterStreamStageWithKey_thenStateObjectIsNotReinitialized(streamStageWithKey
                -> streamStageWithKey.flatMapStateful(
                        TTL,
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            counterValues.add(atomicLong.getAndIncrement());
                            return Traversers.singleton(input.sequence());
                        },
                        (atomicLong, key, wm) -> {
                            evictCount.incrementAndGet();
                            return Traversers.empty();
                        }));
    }

    @Test
    public void mapStateful_whenWindowingAfterStreamStageWithKeyAndEvict_thenStateObjectIsReinitialized() {
        whenWindowingAfterStreamStageWithKeyAndEvict_thenStateObjectIsReinitialized(streamStageWithKey
                -> streamStageWithKey.mapStateful(
                        TTL,
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            if (key == 0) {
                                counterValues.add(atomicLong.getAndIncrement());
                                return input.sequence();
                            } else {
                                return null;
                            }
                        },
                        (atomicLong, key, wm) -> {
                            if (key == 1) {
                                evictCount.incrementAndGet();
                            }
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_whenWindowingAfterStreamStageWithKeyAndEvict_thenStateObjectIsReinitialized() {
        whenWindowingAfterStreamStageWithKeyAndEvict_thenStateObjectIsReinitialized(streamStageWithKey
                -> streamStageWithKey.flatMapStateful(
                        TTL,
                        AtomicLong::new,
                        (atomicLong, key, input) -> {
                            if (key == 0) {
                                counterValues.add(atomicLong.getAndIncrement());
                                return Traversers.singleton(input.sequence());
                            } else {
                                return Traversers.empty();
                            }
                        },
                        (atomicLong, key, wm) -> {
                            if (key == 1) {
                                evictCount.incrementAndGet();
                            }
                            return Traversers.empty();
                        }));
    }

    private void whenWindowingBeforeGeneralStage_thenStateObjectIsNotReinitialized(
            Function<StreamStage<WindowResult<Long>>, StreamStage<WindowResult<Long>>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStage<WindowResult<Long>> streamStage = p.drawFrom(TestSources.itemStream(ITEMS_COUNT))
                .withTimestamps(t -> t.sequence() * TS_MULTIPLY_FACTOR, 0)
                .window(WindowDefinition.tumbling(WINDOW_SIZE))
                .aggregate(counting());
        StreamStage<WindowResult<Long>> statefulStage = statefulFn.apply(streamStage);
        statefulStage.drainTo(Sinks.list(SINK_NAME));

        runAndAssertWindowingGeneralStage(p, WINDOWS_COUNT);
    }

    private void whenWindowingAfterGeneralStage_thenStateObjectIsNotReinitialized(
            Function<StreamStage<SimpleEvent>, StreamStage<SimpleEvent>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStage<SimpleEvent> streamStage = p.drawFrom(TestSources.itemStream(ITEMS_COUNT))
                .withTimestamps(t -> t.sequence() * TS_MULTIPLY_FACTOR, 0);
        StreamStage<SimpleEvent> statefulStage = statefulFn.apply(streamStage);
        statefulStage.window(WindowDefinition.tumbling(WINDOW_SIZE))
                .aggregate(counting())
                .drainTo(Sinks.list(SINK_NAME));

        runAndAssertWindowingGeneralStage(p, ITEMS_COUNT);
    }

    private void whenWindowingAfterGeneralStageWithKey_thenStateObjectIsNotReinitialized(
            Function<StreamStageWithKey<SimpleEvent, Integer>, StreamStage<Long>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<SimpleEvent, Integer> streamStageWithKey = p.drawFrom(TestSources.itemStream(ITEMS_COUNT))
                .withTimestamps(t -> t.sequence() * TS_MULTIPLY_FACTOR, 0)
                .groupingKey(t -> 0);
        StreamStage<Long> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.window(WindowDefinition.tumbling(WINDOW_SIZE))
                .aggregate(counting())
                .drainTo(Sinks.list(SINK_NAME));

        runAndAssertWindowingGeneralStage(p, ITEMS_COUNT);
    }

    private void whenWindowingAfterStreamStageWithKey_thenStateObjectIsNotReinitialized(
            Function<StreamStageWithKey<SimpleEvent, Integer>, StreamStage<Long>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<SimpleEvent, Integer> streamStageWithKey = p.drawFrom(TestSources.itemStream(ITEMS_COUNT))
                .withTimestamps(t -> t.sequence() * TS_MULTIPLY_FACTOR, 0)
                .groupingKey(t -> 0);
        StreamStage<Long> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.window(WindowDefinition.tumbling(WINDOW_SIZE))
                .aggregate(counting())
                .drainTo(Sinks.list(SINK_NAME));

        runAndAssertWindowingGeneralStage(p, ITEMS_COUNT);

        assertEquals(0, evictCount.get());
    }

    private void runAndAssertWindowingGeneralStage(Pipeline p, int minimalMapFnCalls) {
        List<Long> sink = instance.getList(SINK_NAME);
        assertTrue(sink.isEmpty());

        Job job = instance.newJob(p);
        assertTrueEventually(() -> assertTrue(sink.size() >= WINDOWS_COUNT));

        ditchJob(job);

        assertTrue(counterValues.size() >= minimalMapFnCalls);

        for (int i = 0; i < counterValues.size(); i++) {
            assertEquals(new Long(i), counterValues.get(i));
        }
    }

    private void whenWindowingAfterStreamStageWithKeyAndEvict_thenStateObjectIsReinitialized(
            Function<StreamStageWithKey<SimpleEvent, Integer>, StreamStage<Long>> statefulFn) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<SimpleEvent, Integer> streamStageWithKey = p.drawFrom(TestSources.itemStream(ITEMS_COUNT))
                .withTimestamps(t -> t.sequence() * TS_MULTIPLY_FACTOR, 0)
                .groupingKey(t -> t.sequence() % 100 != 0 ? 0 : 1);
        StreamStage<Long> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.window(WindowDefinition.tumbling(WINDOW_SIZE))
                .aggregate(counting())
                .drainTo(Sinks.list(SINK_NAME));

        List<Long> sink = instance.getList(SINK_NAME);
        assertTrue(sink.isEmpty());

        Job job = instance.newJob(p);
        assertTrueEventually(() -> assertTrue(sink.size() >= WINDOWS_COUNT));

        ditchJob(job);

        assertTrue(counterValues.size() >= ITEMS_COUNT);
        assertTrue(evictCount.get() >= 10);

        for (int i = 0; i < counterValues.size(); i++) {
            assertEquals(new Long(i), counterValues.get(i));
        }
    }
}
