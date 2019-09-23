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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class StatefulMappingStressTest extends JetTestSupport {

    private static final Random RANDOM = new Random();
    private static final long TTL = SECONDS.toMillis(2);

    private JetInstance instance;

    @Before
    public void setup() {
        instance = createJetMembers(new JetConfig(), 2)[0];
    }

    @Test
    public void mapStateful_stressTest() {
        AtomicLong functionCallCount = new AtomicLong();
        AtomicLong evictionCallCount = new AtomicLong();
        stressTest(functionCallCount, evictionCallCount,
                streamStageWithKey -> streamStageWithKey.mapStateful(TTL,
                        Object::new,
                        (state, key, input) -> {
                            functionCallCount.incrementAndGet();
                            return null;
                        },
                        (state, key, wm) -> {
                            evictionCallCount.incrementAndGet();
                            return null;
                        }));
    }

    @Test
    public void flatMapStateful_stressTest() {
        AtomicLong functionCallCount = new AtomicLong();
        AtomicLong evictionCallCount = new AtomicLong();
        stressTest(functionCallCount, evictionCallCount,
                streamStageWithKey -> streamStageWithKey.flatMapStateful(TTL,
                        Object::new,
                        (state, key, input) -> {
                            functionCallCount.incrementAndGet();
                            return Traversers.empty();
                        },
                        (state, key, wm) -> {
                            evictionCallCount.incrementAndGet();
                            return Traversers.empty();
                        }));
    }

    private void stressTest(
            AtomicLong functionCallCount,
            AtomicLong evictionCallCount,
            Function<StreamStageWithKey<Integer, Integer>, StreamStage<Object>> statefulFn
    ) {
        int emitItemsCount = 2_000_000;
        Pipeline p = Pipeline.create();
        StreamStageWithKey<Integer, Integer> streamStageWithKey = p.drawFrom(TestSources.itemStream(100_000))
                .withIngestionTimestamps()
                .filter(f -> f.sequence() < emitItemsCount)
                .map(t -> RANDOM.nextInt(100_000))
                .groupingKey(k -> k % 100_000);
        StreamStage<Object> statefulStage = statefulFn.apply(streamStageWithKey);
        statefulStage.drainTo(Sinks.logger());

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(emitItemsCount, functionCallCount.get());
            assertTrue(evictionCallCount.get() > 0);
        });
    }
}
