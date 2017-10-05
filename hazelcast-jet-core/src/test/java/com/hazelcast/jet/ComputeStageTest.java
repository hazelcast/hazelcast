/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.core.IList;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.JoinClause.joinMapEntries;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ComputeStageTest extends JetTestSupport {

    private Pipeline pipeline;
    private ComputeStage<Integer> srcStage;
    private Sink<Object> sink;

    private JetTestInstanceFactory factory;
    private JetInstance jet;
    private IList<Integer> srcList;
    private IList<Object> sinkList;

    @Before
    public void before() {
        pipeline = Pipeline.create();
        srcStage = pipeline.drawFrom(Sources.readList("src"));
        sink = Sinks.writeList("sink");

        factory = new JetTestInstanceFactory();
        jet = factory.newMember();
        srcList = jet.getList("src");
        sinkList = jet.getList("sink");
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }


    @Test(expected = IllegalArgumentException.class)
    public void when_emptyPipelineToDag_then_exceptionInIterator() {
        Pipeline.create().toDag().iterator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_missingSink_then_exceptionInDagIterator() {
        pipeline.toDag().iterator();
    }

    @Test
    public void when_minimalPipeline_then_validDag() {
        srcStage.drainTo(Sinks.writeList("out"));
        assertTrue(pipeline.toDag().iterator().hasNext());
    }

    @Test
    public void map() {
        // Given
        ComputeStage<String> mapped = srcStage.map(Object::toString);
        mapped.drainTo(sink);

        // When
        srcList.add(1);
        execute();

        // Then
        assertEquals(bag("1"), toBag(sinkList));
    }

    @Test
    public void filter() {
        // Given
        ComputeStage<Integer> filtered = srcStage.filter(o -> o.equals(2));
        filtered.drainTo(sink);

        // When
        srcList.add(1);
        srcList.add(2);
        execute();

        // Then
        assertEquals(bag(2), toBag(sinkList));
    }

    @Test
    public void flatMap() {
        // Given
        ComputeStage<String> flatMapped = srcStage.flatMap(o -> traverseIterable(asList(o + "A", o + "B")));
        flatMapped.drainTo(sink);

        // When
        srcList.add(1);
        execute();

        // Then
        assertEquals(bag("1A", "1B"), toBag(sinkList));
    }

    @Test
    public void groupBy() {
        //Given
        ComputeStage<Entry<Integer, Long>> grouped = srcStage.groupBy(wholeItem(), counting());
        grouped.drainTo(sink);

        // When
        srcList.addAll(asList(1, 2, 3, 3, 2, 3));
        execute();

        // Then
        assertEquals(bag(entry(1, 1L), entry(2, 2L), entry(3, 3L)), toBag(sinkList));
    }

    @Test
    public void hashJoinTwo() {
        // Given
        ComputeStage<Map.Entry<Integer, String>> enrichingStage = pipeline.drawFrom(Sources.readList("enrich"));

        ComputeStage<Tuple2<Integer, String>> joined = srcStage.hashJoin(enrichingStage, joinMapEntries(wholeItem()));
        joined.drainTo(sink);

        // When
        srcList.addAll(asList(1, 2, 1));
        jet.getList("enrich").addAll(asList(entry(1, "one"), entry(2, "two")));
        execute();

        // Then
        assertEquals(bag(tuple2(1, "one"), tuple2(2, "two"), tuple2(1, "one")), toBag(sinkList));
    }

    @Test
    public void hashJoinThree() {
        // Given
        ComputeStage<Map.Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.readList("enrich1"));
        ComputeStage<Map.Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.readList("enrich2"));
        ComputeStage<Tuple3<Integer, String, String>> joined = srcStage.hashJoin(
                enrichingStage1, joinMapEntries(wholeItem()),
                enrichingStage2, joinMapEntries(wholeItem())
        );
        joined.drainTo(sink);

        // When
        srcList.addAll(asList(1, 2, 1));
        jet.getList("enrich1").addAll(asList(entry(1, "one"), entry(2, "two")));
        jet.getList("enrich2").addAll(asList(entry(1, "uno"), entry(2, "due")));
        execute();

        // Then
        assertEquals(bag(tuple3(1, "one", "uno"), tuple3(2, "two", "due"), tuple3(1, "one", "uno")), toBag(sinkList));
    }

    @Test
    public void hashJoinBuilder() {
        // Given
        ComputeStage<Map.Entry<Integer, String>> enrichingStage1 = pipeline.drawFrom(Sources.readList("enrich1"));
        ComputeStage<Map.Entry<Integer, String>> enrichingStage2 = pipeline.drawFrom(Sources.readList("enrich2"));
        HashJoinBuilder<Integer> b = srcStage.hashJoinBuilder();
        Tag<String> english = b.add(enrichingStage1, joinMapEntries(wholeItem()));
        Tag<String> italian = b.add(enrichingStage2, joinMapEntries(wholeItem()));
        ComputeStage<Tuple2<Integer, ItemsByTag>> joined = b.build();
        joined.drainTo(sink);

        // When
        srcList.addAll(asList(1, 2, 1));
        jet.getList("enrich1").addAll(asList(entry(1, "one"), entry(2, "two")));
        jet.getList("enrich2").addAll(asList(entry(1, "uno"), entry(2, "due")));
        execute();

        // Then
        Tuple2<Integer, ItemsByTag> one = tuple2(1, itemsByTag(english, "one", italian, "uno"));
        Tuple2<Integer, ItemsByTag> two = tuple2(2, itemsByTag(english, "two", italian, "due"));
        assertEquals(bag(one, two, one), toBag(sinkList));
    }

    @Test
    public void coGroupTwo() {
        //Given
        ComputeStage<Integer> src1 = pipeline.drawFrom(Sources.readList("src1"));

        ComputeStage<Entry<Integer, Long>> coGrouped = srcStage.coGroup(wholeItem(), src1, wholeItem(),
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> oneTwoThree = asList(1, 2, 2, 3, 3, 3);
        srcList.addAll(oneTwoThree);
        jet.getList("src1").addAll(oneTwoThree);
        execute();

        // Then
        assertEquals(bag(entry(1, 11L), entry(2, 22L), entry(3, 33L)), toBag(sinkList));
    }

    @Test
    public void coGroupThree() {
        //Given
        ComputeStage<Integer> src1 = pipeline.drawFrom(Sources.readList("src1"));
        ComputeStage<Integer> src2 = pipeline.drawFrom(Sources.readList("src2"));

        ComputeStage<Entry<Integer, Long>> coGrouped = srcStage.coGroup(wholeItem(),
                src1, wholeItem(),
                src2, wholeItem(),
                AggregateOperation
                        .withCreate(LongAccumulator::new)
                        .andAccumulate0((count, item) -> count.add(1))
                        .andAccumulate1((count, item) -> count.add(10))
                        .andAccumulate2((count, item) -> count.add(100))
                        .andCombine(LongAccumulator::add)
                        .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> oneTwoThree = asList(1, 2, 2, 3, 3, 3);
        srcList.addAll(oneTwoThree);
        jet.getList("src1").addAll(oneTwoThree);
        jet.getList("src2").addAll(oneTwoThree);
        execute();

        // Then
        assertEquals(bag(entry(1, 111L), entry(2, 222L), entry(3, 333L)), toBag(sinkList));
    }

    @Test
    public void coGroupBuilder() {
        //Given
        ComputeStage<Integer> src1 = pipeline.drawFrom(Sources.readList("src1"));
        ComputeStage<Integer> src2 = pipeline.drawFrom(Sources.readList("src2"));
        CoGroupBuilder<Integer, Integer> b = srcStage.coGroupBuilder(wholeItem());
        Tag<Integer> tag0 = b.tag0();
        Tag<Integer> tag1 = b.add(src1, wholeItem());
        Tag<Integer> tag2 = b.add(src2, wholeItem());
        ComputeStage<Tuple2<Integer, Long>> coGrouped = b.build(AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0, (count, item) -> count.add(1))
                .andAccumulate(tag1, (count, item) -> count.add(10))
                .andAccumulate(tag2, (count, item) -> count.add(100))
                .andCombine(LongAccumulator::add)
                .andFinish(LongAccumulator::get));
        coGrouped.drainTo(sink);

        // When
        List<Integer> oneTwoThree = asList(1, 2, 2, 3, 3, 3);
        srcList.addAll(oneTwoThree);
        jet.getList("src1").addAll(oneTwoThree);
        jet.getList("src2").addAll(oneTwoThree);
        execute();

        // Then
        assertEquals(bag(entry(1, 111L), entry(2, 222L), entry(3, 333L)), toBag(sinkList));
    }

    @Test
    public void customTransform() {
        // Given
        ComputeStage<Object> custom = srcStage.customTransform("map", Processors.mapP(Object::toString));
        custom.drainTo(sink);

        // When
        srcList.add(1);
        execute();

        // Then
        assertEquals(bag("1"), toBag(sinkList));
    }

    private void execute() {
        jet.newJob(pipeline).join();
    }

    @SafeVarargs
    private static <T> Map<T, Integer> bag(T... elems) {
        return toBag(asList(elems));
    }

    private static <T> Map<T, Integer> toBag(Collection<T> coll) {
        Map<T, Integer> bag = new HashMap<>();
        for (T t : coll) {
            bag.merge(t, 1, (count, x) -> count + 1);
        }
        return bag;
    }
}
