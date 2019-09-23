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
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static com.hazelcast.jet.Util.entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class StatefulMappingEvictionTest extends JetTestSupport {

    private static final long TTL = SECONDS.toMillis(2);
    private static final long TS_FREQUENCY = 100;
    // it must be value like TTL_INTERVAL_LOWER * 1.1 < TS_FREQUENCY
    private static final long TTL_INTERVAL_LOWER = 90;
    private static final long TTL_INTERVAL_EQUAL = TS_FREQUENCY;

    private static final String SINK_MAP_NAME = StatefulMappingEvictionTest.class.getSimpleName() + "_map";
    private static AtomicBoolean emitSpecialItem;
    private static AtomicLong evictCount;
    private static List<Long> keys;
    private static List<Long> mapFnCounterValues;
    private static List<Long> evictCounterValues;

    private JetInstance instance;

    @Before
    public void setup() {
        emitSpecialItem = new AtomicBoolean(false);
        keys = new ArrayList<>();
        mapFnCounterValues = new ArrayList<>();
        evictCounterValues = new ArrayList<>();
        evictCount = new AtomicLong(0);
        instance = createJetMembers(new JetConfig(), 2)[0];
    }

    @Test
    public void mapStateful_whenEvictedAndAnotherItem_thenJustRelatedStateIsReinitialized() {
        whenEvictedAndAnotherItem_thenJustRelatedStateIsReinitialized(streamStageWithKey ->
                streamStageWithKey.mapStateful(TTL,
                        () -> new long[1],
                        (counter, key, input) -> {
                            if (key == 1) {
                                keys.add(input);
                            }
                            return entry(input, 2 * counter[0]++);
                        },
                        (counter, key, wm) -> {
                            if (key == 1) {
                                evictCounterValues.add(counter[0]);
                                if (evictCount.getAndIncrement() < 1) {
                                    emitSpecialItem.set(true);
                                }
                            }
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_whenEvictedAndAnotherItem_thenJustRelatedStateIsReinitialized() {
        whenEvictedAndAnotherItem_thenJustRelatedStateIsReinitialized(streamStageWithKey ->
                streamStageWithKey.flatMapStateful(TTL,
                        () -> new long[1],
                        (counter, key, input) -> {
                            if (key == 1) {
                                keys.add(input);
                            }
                            return Traversers.singleton(entry(input, 2 * counter[0]++));
                        },
                        (counter, key, wm) -> {
                            if (key == 1) {
                                evictCounterValues.add(counter[0]);
                                if (evictCount.getAndIncrement() < 1) {
                                    emitSpecialItem.set(true);
                                }
                            }
                            return Traversers.empty();
                        })
        );
    }

    @Test
    public void mapStateful_whenItemIntervalIsHigherThanTtl_thenEvictedBeforeEveryItem() {
        whenItemIntervalIsHigherThanTtl_thenEvictedBeforeEveryItem(streamStageWithKey ->
                streamStageWithKey.mapStateful(
                        TTL_INTERVAL_LOWER,
                        () -> new long[1],
                        (counter, key, input) -> {
                            mapFnCounterValues.add(counter[0]++);
                            return entry(input.sequence(), input.sequence());
                        },
                        (counter, key, wm) -> {
                            evictCounterValues.add(counter[0]++);
                            evictCount.incrementAndGet();
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_whenItemIntervalIsHigherThanTtl_thenEvictedBeforeEveryItem() {
        whenItemIntervalIsHigherThanTtl_thenEvictedBeforeEveryItem(streamStageWithKey ->
                streamStageWithKey.flatMapStateful(
                        TTL_INTERVAL_LOWER,
                        () -> new long[1],
                        (counter, key, input) -> {
                            mapFnCounterValues.add(counter[0]++);
                            return Traversers.singleton(entry(input.sequence(), input.sequence()));
                        },
                        (counter, key, wm) -> {
                            evictCounterValues.add(counter[0]++);
                            evictCount.incrementAndGet();
                            return Traversers.empty();
                        }));
    }

    @Test
    public void mapStateful_whenItemIntervalIsEqualTtl_thenEvictionDoesNotHappen() {
        whenItemIntervalIsEqualTtl_thenEvictionDoesNotHappen(streamStageWithKey ->
                streamStageWithKey.mapStateful(
                        TTL_INTERVAL_EQUAL,
                        () -> new long[1],
                        (counter, key, input) -> entry(input.sequence(), input.sequence()),
                        (counter, key, wm) -> {
                            evictCount.incrementAndGet();
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_whenItemIntervalIsEqualTtl_thenEvictionDoesNotHappen() {
        whenItemIntervalIsEqualTtl_thenEvictionDoesNotHappen(streamStageWithKey ->
                streamStageWithKey.flatMapStateful(
                        TTL_INTERVAL_EQUAL,
                        () -> new long[1],
                        (counter, key, input) ->
                                Traversers.singleton(entry(input.sequence(), input.sequence())),
                        (counter, key, wm) -> {
                            evictCount.incrementAndGet();
                            return Traversers.empty();
                        }));
    }

    private void whenItemIntervalIsHigherThanTtl_thenEvictedBeforeEveryItem(
            Function<StreamStageWithKey<SimpleEvent, Object>, StreamStage<Map.Entry<Long, Long>>> statefulFn
    ) {
        runIntervalTest(statefulFn,
                minimumSize -> {
                    assertTrue(evictCount.get() >= minimumSize);
                    for (long mapFnCounterValue : mapFnCounterValues) {
                        assertEquals(0, mapFnCounterValue);
                    }
                    for (long evictCounterValue : evictCounterValues) {
                        assertEquals(1, evictCounterValue);
                    }
                });
    }

    private void whenItemIntervalIsEqualTtl_thenEvictionDoesNotHappen(
            Function<StreamStageWithKey<SimpleEvent, Object>, StreamStage<Map.Entry<Long, Long>>> statefulFn
    ) {
        runIntervalTest(statefulFn, ignored -> assertEquals(0, evictCount.get()));
    }

    private void runIntervalTest(
            Function<StreamStageWithKey<SimpleEvent, Object>, StreamStage<Map.Entry<Long, Long>>> statefulFn,
            LongConsumer assertion
    ) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<SimpleEvent, Object> streamStageWithKey = p
                .drawFrom(TestSources.itemStream(1000))
                .withTimestamps(t -> t.sequence() * TS_FREQUENCY, 0)
                .groupingKey(t -> 0L);
        StreamStage<Map.Entry<Long, Long>> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.drainTo(Sinks.map(SINK_MAP_NAME));

        Map<Long, Long> map = instance.getMap(SINK_MAP_NAME);
        assertTrue(map.isEmpty());

        Job job = instance.newJob(p);
        assertTrueEventually(() -> assertTrue(map.size() >= 100));

        ditchJob(job);

        assertTrueEventually(() -> assertion.accept(map.size() - 1));
    }

    private void whenEvictedAndAnotherItem_thenJustRelatedStateIsReinitialized(
            Function<StreamStageWithKey<Long, Long>, StreamStage<Map.Entry<Long, Long>>> statefulFn
    ) {
        Pipeline p = Pipeline.create();
        StreamStageWithKey<Long, Long> streamStageWithKey = p
                .drawFrom(TestSources.itemStream(1000))
                .withIngestionTimestamps()
                .setLocalParallelism(1)
                .map(t -> emitSpecialItem.getAndSet(false) ? t.sequence() * 2 + 1 : t.sequence() * 2)
                .groupingKey(t -> t % 2);
        StreamStage<Map.Entry<Long, Long>> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.drainTo(Sinks.map(SINK_MAP_NAME));

        Map<Long, Long> map = instance.getMap(SINK_MAP_NAME);
        assertTrue(map.isEmpty());

        Job job = instance.newJob(p);
        assertTrueEventually(() -> assertFalse(map.isEmpty()));
        emitSpecialItem.set(true);

        // when
        assertTrueEventually(() -> assertTrue(evictCount.get() > 1));

        ditchJob(job);

        // then
        for (long evictCounterValue : evictCounterValues) {
            assertEquals(1, evictCounterValue);
        }

        assertEquals(2, keys.size());
        for (Long key : keys) {
            assertEquals((Long) 0L, map.get(key));
        }

        long specialIndex1 = keys.get(0) / 2;
        long specialIndex2 = keys.get(1) / 2;
        int initializedCounter = 0;
        for (long i = 0; i < map.size(); i++) {
            if (i != specialIndex1 && i != specialIndex2) {
                if (map.get(i * 2) == 0) {
                    initializedCounter++;
                }
            }
        }
        assertEquals("State object should be initialized only once for even items.", 1, initializedCounter);
    }
}
