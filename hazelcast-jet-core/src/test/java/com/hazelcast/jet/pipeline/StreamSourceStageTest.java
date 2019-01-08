/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

import static com.hazelcast.jet.function.DistributedPredicate.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
public class StreamSourceStageTest extends StreamSourceStageTestBase {

    @BeforeClass
    public static void beforeClass1() {
        IMap<Integer, Integer> sourceMap = instances[0].getMap(JOURNALED_MAP_NAME);
        sourceMap.put(1, 1);
        sourceMap.put(2, 2);
    }

    private static StreamSource<Integer> createSourceJournal() {
        return Sources.<Integer, Integer, Integer>mapJournal(JOURNALED_MAP_NAME, alwaysTrue(),
                EventJournalMapEvent::getKey, START_FROM_OLDEST);
    }

    private static StreamSource<Integer> createTimestampedSourceBuilder() {
        return SourceBuilder.timestampedStream("s", ctx -> new boolean[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (!ctx[0]) {
                        buf.add(1, 1);
                        buf.add(2, 2);
                        ctx[0] = true;
                    }
                })
                .build();
    }

    private static StreamSource<Integer> createPlainSourceBuilder() {
        return SourceBuilder.stream("s", ctx -> new boolean[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (!ctx[0]) {
                        buf.add(1);
                        buf.add(2);
                        ctx[0] = true;
                    }
                })
                .build();
    }

    @Test
    public void test_timestampedSourceBuilder_withoutTimestamps() {
        test(createTimestampedSourceBuilder(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_timestampedSourceBuilder_withNativeTimestamps() {
        test(createTimestampedSourceBuilder(), withNativeTimestampsFn, asList(1L, 2L), null);
    }

    @Test
    public void test_timestampedSourceBuilder_withTimestamps() {
        test(createTimestampedSourceBuilder(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_plainSourceBuilder_withoutTimestamps() {
        test(createPlainSourceBuilder(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_plainSourceBuilder_withNativeTimestamps() {
        test(createPlainSourceBuilder(), withNativeTimestampsFn, emptyList(),
                "The source doesn't support native timestamps");
    }

    @Test
    public void test_plainSourceBuilder_withTimestamps() {
        test(createPlainSourceBuilder(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withoutTimestamps() {
        test(createSourceJournal(), withoutTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_sourceJournal_withNativeTimestamps() {
        test(createSourceJournal(), withNativeTimestampsFn, emptyList(),
                "The source doesn't support native timestamps");
    }

    @Test
    public void test_sourceJournal_withTimestamps() {
        test(createSourceJournal(), withTimestampsFn, asList(2L, 3L), null);
    }

    @Test
    public void test_withTimestampsButTimestampsNotUsed() {
        IList sinkList = instances[0].getList("sinkList");

        Pipeline p = Pipeline.create();
        p.drawFrom(createSourceJournal())
         .withIngestionTimestamps()
         .drainTo(Sinks.list(sinkList));

        instances[0].newJob(p);
        assertTrueEventually(() -> assertEquals(Arrays.asList(1, 2), new ArrayList<>(sinkList)), 5);
    }

    @Test
    public void when_withTimestampsAndAddTimestamps_then_fail() {
        Pipeline p = Pipeline.create();
        StreamStage<Entry<Object, Object>> stage = p.drawFrom(Sources.mapJournal("foo", START_FROM_OLDEST))
                                                  .withIngestionTimestamps();

        expectedException.expectMessage("This stage already has timestamps assigned to it");
        stage.addTimestamps(o -> 0L, 0);
    }
}
