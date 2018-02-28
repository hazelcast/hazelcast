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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.pipeline.AggregateBuilder;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.core.TestUtil.set;
import static com.hazelcast.jet.core.test.TestSupport.listToString;
import static com.hazelcast.jet.datamodel.ThreeBags.threeBags;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(ParallelTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AggregateTransform_IntegrationTest extends JetTestSupport {

    @Test
    public void test_aggregate() {
        // Given
        JetInstance instance = createJetMember();
        IMap<Long, String> map = instance.getMap("source");
        map.put(0L, "foo");
        map.put(1L, "bar");

        // When
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>map("source"))
         .aggregate(toSet())
         .drainTo(Sinks.list("sink"));
        instance.newJob(p).join();

        //Then
        assertEquals(
                listToString(singletonList(set(entry(0L, "foo"), entry(1L, "bar")))),
                listToString(instance.getHazelcastInstance().getList("sink")));
    }

    @Test
    public void test_aggregate2() {
        // Given
        JetInstance instance = createJetMember();
        IListJet<Integer> list = instance.getList("list");
        list.add(1);
        list.add(2);
        list.add(3);
        IListJet<String> list1 = instance.getList("list1");
        list1.add("a");
        list1.add("b");
        list1.add("c");

        // When
        Pipeline p = Pipeline.create();
        BatchStage<Integer> stage1 = p.drawFrom(Sources.list("list1"));
        p.drawFrom(Sources.list("list"))
         .aggregate2(stage1, toTwoBags())
         .drainTo(Sinks.list("sink"));
        instance.newJob(p).join();

        //Then
        TwoBags twoBags = (TwoBags) instance.getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(twoBags);
        sort(twoBags);
        assertEquals(TwoBags.twoBags(list, list1), twoBags);
    }

    @Test
    public void test_aggregate3() {
        // Given
        JetInstance instance = createJetMember();
        IListJet<Integer> list = instance.getList("list");
        list.add(1);
        list.add(2);
        list.add(3);
        IListJet<String> list1 = instance.getList("list1");
        list1.add("a");
        list1.add("b");
        list1.add("c");
        IListJet<Double> list2 = instance.getList("list2");
        list2.add(6.0d);
        list2.add(7.0d);
        list2.add(8.0d);

        // When
        Pipeline p = Pipeline.create();
        BatchStage<String> stage1 = p.drawFrom(Sources.list("list1"));
        BatchStage<Double> stage2 = p.drawFrom(Sources.list("list2"));
        p.drawFrom(Sources.list("list"))
         .aggregate3(stage1, stage2, toThreeBags())
         .drainTo(Sinks.list("sink"));
        instance.newJob(p).join();

        //Then
        ThreeBags threeBags = (ThreeBags) instance.getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(threeBags);
        sort(threeBags);
        assertEquals(threeBags(list, list1, list2), threeBags);
    }


    @Test
    public void test_aggregate3_with_aggBuilder() {
        // Given
        JetInstance instance = createJetMember();
        IListJet<Integer> list = instance.getList("list");
        list.add(1);
        list.add(2);
        list.add(3);
        IListJet<String> list1 = instance.getList("list1");
        list1.add("a");
        list1.add("b");
        list1.add("c");
        IListJet<Double> list2 = instance.getList("list2");
        list2.add(6.0d);
        list2.add(7.0d);
        list2.add(8.0d);

        // When
        Pipeline p = Pipeline.create();
        BatchStage<Integer> stage0 = p.drawFrom(Sources.list("list"));
        BatchStage<String> stage1 = p.drawFrom(Sources.list("list1"));
        BatchStage<Double> stage2 = p.drawFrom(Sources.list("list2"));


        AggregateBuilder<Integer> builder = stage0.aggregateBuilder();
        Tag<Integer> tag0 = builder.tag0();
        Tag<String> tag1 = builder.add(stage1);
        Tag<Double> tag2 = builder.add(stage2);

        BatchStage<ThreeBags> resultStage = builder.build(AggregateOperation
                .withCreate(ThreeBags::threeBags)
                .andAccumulate(tag0, (acc, item0) -> acc.bag0().add(item0))
                .andAccumulate(tag1, (acc, item1) -> acc.bag1().add(item1))
                .andAccumulate(tag2, (acc, item2) -> acc.bag2().add(item2))
                .andCombine(ThreeBags::combineWith)
                .andDeduct(ThreeBags::deduct)
                .andFinish(ThreeBags::finish));

        resultStage.drainTo(Sinks.list("sink"));
        instance.newJob(p).join();

        //Then
        ThreeBags threeBags = (ThreeBags) instance.getHazelcastInstance().getList("sink").iterator().next();
        assertNotNull(threeBags);
        sort(threeBags);
        assertEquals(threeBags(list, list1, list2), threeBags);
    }

    public void sort(ThreeBags threeBags) {
        Collections.sort((List) threeBags.bag0());
        Collections.sort((List) threeBags.bag1());
        Collections.sort((List) threeBags.bag2());
    }

    public void sort(TwoBags twoBags) {
        Collections.sort((List) twoBags.bag0());
        Collections.sort((List) twoBags.bag1());
    }
}
