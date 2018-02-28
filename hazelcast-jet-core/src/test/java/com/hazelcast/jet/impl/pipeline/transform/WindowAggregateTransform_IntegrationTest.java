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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import java.util.Map.Entry;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.core.TestUtil.set;
import static com.hazelcast.jet.core.test.TestSupport.listToString;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowAggregateTransform_IntegrationTest extends JetTestSupport {

    private JetInstance instance;

    @Before
    public void before() {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(
                new EventJournalConfig().setMapName("source*").setEnabled(true));
        config.getHazelcastConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        instance = createJetMember(config);
    }

    @Test
    public void testTumbling() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(1L, "bar");
        map.put(2L, "baz");
        map.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedItem<>(2, set(entry(0L, "foo"), entry(1L, "bar"))),
                            new TimestampedItem<>(4, set(entry(2L, "baz"))))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }

    @Test
    public void testSession() {
        IMap<Long, String> map = instance.getMap("source");
        map.put(0L, "foo");
        map.put(3L, "bar");
        map.put(4L, "baz");
        map.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.session(2))
         .aggregate(toSet(), (winStart, winEnd, result) -> new WindowResult<>(winStart, winEnd, "", result))
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);

        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new WindowResult<>(0, 2, "", set(entry(0L, "foo"))),
                            new WindowResult<>(3, 6, "", set(entry(3L, "bar"), entry(4L, "baz"))))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);
    }

    @Test
    public void test_aggregate2() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(2L, "baz");
        map.put(10L, "flush-item");

        IMap<Long, String> map2 = instance.getMap("source1");
        // key is timestamp
        map2.put(0L, "faa");
        map2.put(2L, "buu");
        map2.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        StreamStage<Entry<Long, String>> stage1 = p.drawFrom(
                Sources.<Long, String>mapJournal("source1", START_FROM_OLDEST));
        stage1.addTimestamps(Entry::getKey, 0);

        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .aggregate2(stage1, toTwoBags())
         .peek()
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedItem<>(2, TwoBags.twoBags(
                                    asList(entry(0L, "foo")),
                                    asList(entry(0L, "faa"))
                            )),
                            new TimestampedItem<>(4, TwoBags.twoBags(
                                    asList(entry(2L, "baz")),
                                    asList(entry(2L, "buu"))
                            )))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);

    }

    @Test
    public void test_aggregate3() {
        IMap<Long, String> map = instance.getMap("source");
        // key is timestamp
        map.put(0L, "foo");
        map.put(2L, "caz");
        map.put(10L, "flush-item");

        IMap<Long, String> map2 = instance.getMap("source1");
        // key is timestamp
        map2.put(0L, "faa");
        map2.put(2L, "cuu");
        map2.put(10L, "flush-item");


        IMap<Long, String> map3 = instance.getMap("source2");
        // key is timestamp
        map3.put(0L, "fzz");
        map3.put(2L, "ccc");
        map3.put(10L, "flush-item");

        Pipeline p = Pipeline.create();
        StreamStage<Entry<Long, String>> stage1 = p.drawFrom(
                Sources.<Long, String>mapJournal("source1", START_FROM_OLDEST));
        stage1.addTimestamps(Entry::getKey, 0);
        StreamStage<Entry<Long, String>> stage2 = p.drawFrom(
                Sources.<Long, String>mapJournal("source2", START_FROM_OLDEST));
        stage2.addTimestamps(Entry::getKey, 0);

        p.drawFrom(Sources.<Long, String>mapJournal("source", START_FROM_OLDEST))
         .addTimestamps(Entry::getKey, 0)
         .window(WindowDefinition.tumbling(2))
         .aggregate3(stage1, stage2, toThreeBags())
         .peek()
         .drainTo(Sinks.list("sink"));

        instance.newJob(p);
        assertTrueEventually(() -> {
            assertEquals(
                    listToString(asList(
                            new TimestampedItem<>(2, ThreeBags.threeBags(
                                    asList(entry(0L, "foo")),
                                    asList(entry(0L, "faa")),
                                    asList(entry(0L, "fzz"))
                            )),
                            new TimestampedItem<>(4, ThreeBags.threeBags(
                                    asList(entry(2L, "caz")),
                                    asList(entry(2L, "cuu")),
                                    asList(entry(2L, "ccc"))
                            )))),
                    listToString(instance.getHazelcastInstance().getList("sink")));
        }, 5);

    }
}
