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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class StatefulMappingStressTest extends JetTestSupport {

    private static final Random RANDOM = new Random();
    private static final long TTL = SECONDS.toMillis(2);
    private static final String MAP_SINK_NAME = StatefulMappingStressTest.class.getSimpleName() + "_sink";

    private HazelcastInstance instance;

    @Before
    public void setup() {
        instance = createHazelcastInstances(defaultInstanceConfigWithJetEnabled(), 2)[0];
    }

    @Test
    public void mapStateful_stressTest() {
        stressTest(
                streamStageWithKey -> streamStageWithKey.mapStateful(TTL,
                        Object::new,
                        (state, key, input) -> {
                            return entry(0, 1);
                        },
                        (state, key, wm) -> {
                            return entry(1, 1);
                        }));
    }

    @Test
    public void flatMapStateful_stressTest() {
        stressTest(
                streamStageWithKey -> streamStageWithKey.flatMapStateful(TTL,
                        Object::new,
                        (state, key, input) -> {
                            return Traversers.singleton(entry(0, 1));
                        },
                        (state, key, wm) -> {
                            return Traversers.singleton(entry(1, 1));
                        }));
    }

    private void stressTest(
            Function<StreamStageWithKey<Integer, Integer>, StreamStage<Map.Entry<Integer, Integer>>> statefulFn
    ) {
        int emitItemsCount = 2_000_000;
        Pipeline p = Pipeline.create();
        StreamStageWithKey<Integer, Integer> streamStageWithKey = p.readFrom(TestSources.itemStream(100_000))
                .withIngestionTimestamps()
                .filter(f -> f.sequence() < emitItemsCount)
                .map(t -> RANDOM.nextInt(100_000))
                .groupingKey(k -> k % 100_000);
        StreamStage<Map.Entry<Integer, Integer>> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.writeTo(Sinks.mapWithMerging(MAP_SINK_NAME, (oldValue, newValue) -> oldValue + newValue));

        instance.getJet().newJob(p);

        Map<Integer, Integer> map = instance.getMap(MAP_SINK_NAME);

        assertTrueEventually(() -> {
            assertNotNull(map.get(0));
            assertNotNull(map.get(1));
            assertEquals(emitItemsCount, map.get(0).intValue());
            assertTrue(map.get(1) > 0);
        });
    }
}
