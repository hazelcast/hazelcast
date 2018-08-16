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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.TriFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BatchAggregateTest extends PipelineTestSupport {

    private BatchStage<Integer> srcStage;

    @Before
    public void before() {
        srcStage = p.drawFrom(mapValuesSource(srcName));
    }

    @Test
    public void aggregate() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);

        // When
        BatchStage<Set<Integer>> aggregated = srcStage.aggregate(toSet());

        //Then
        aggregated.drainTo(sink);
        execute();
        assertEquals(toBag(singletonList(new HashSet<>(input))), sinkToBag());
    }

    @Test
    public void aggregate2_withSeparateAggrOps() {
        testAggregate2(stage1 -> srcStage.aggregate2(toList(), stage1, toList()));
    }

    @Test
    public void aggregate2_withAggrOp2() {
        testAggregate2(stage1 -> srcStage.aggregate2(stage1, aggregateOperation2(toList(), toList())));
    }

    private void testAggregate2(
            Function<BatchStage<String>, BatchStage<Tuple2<List<Integer>, List<String>>>> attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn = i -> i + "-x";
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn);

        // When
        BatchStage<Tuple2<List<Integer>, List<String>>> aggregated = attachAggregatingStageFn.apply(stage1);

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        Tuple2<List<Integer>, List<String>> actual = (Tuple2<List<Integer>, List<String>>) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(input), toBag(actual.f0()));
        assertEquals(toBag(input.stream().map(mapFn).collect(Collectors.toList())), toBag(actual.f1()));
    }

    @Test
    public void aggregate2_withSeparateAggrOps_withOutputFn() {
        testAggregate2_withOutputFn(stage1 ->
                srcStage.aggregate2(toList(), stage1, toList(), (r0, r1) -> r0.size() + r1.size()));
    }

    @Test
    public void aggregate2_withAggrOp2_with_finishFn() {
        testAggregate2_withOutputFn(stage1 -> srcStage.aggregate2(stage1,
                aggregateOperation2(toList(), toList(), (r0, r1) -> r0.size() + r1.size())));
    }

    private void testAggregate2_withOutputFn(
            Function<BatchStage<Integer>, BatchStage<Integer>> attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        BatchStage<Integer> stage1 = p.drawFrom(mapValuesSource(src1Name));

        // When
        BatchStage<Integer> aggregated = attachAggregatingStageFn.apply(stage1);

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        int actual = (Integer) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(2 * input.size(), actual);
    }

    @Test
    public void aggregate3_withSeparateAggrOps() {
        testAggregate3((stage1, stage2) -> srcStage.aggregate3(toList(), stage1, toList(), stage2, toList()));
    }

    @Test
    public void aggregate3_withAggrOp3() {
        testAggregate3((stage1, stage2) -> srcStage.aggregate3(stage1, stage2,
                aggregateOperation3(toList(), toList(), toList())));
    }

    private void testAggregate3(
            BiFunction<BatchStage<String>, BatchStage<String>,
                    BatchStage<Tuple3<List<Integer>, List<String>, List<String>>>> attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn1 = i -> i + "-a";
        DistributedFunction<Integer, String> mapFn2 = i -> i + "-b";
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn1);
        BatchStage<String> stage2 = p.drawFrom(mapValuesSource(src2Name)).map(mapFn2);

        // When
        BatchStage<Tuple3<List<Integer>, List<String>, List<String>>> aggregated =
                attachAggregatingStageFn.apply(stage1, stage2);

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        Tuple3<List<Integer>, List<String>, List<String>> actual = (Tuple3<List<Integer>, List<String>, List<String>>)
                sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(input), toBag(actual.f0()));
        assertEquals(toBag(input.stream().map(mapFn1).collect(Collectors.toList())), toBag(actual.f1()));
        assertEquals(toBag(input.stream().map(mapFn2).collect(Collectors.toList())), toBag(actual.f2()));
    }

    @Test
    public void aggregate3_withSeparateAggrOps_withOutputFn() {
        testAggregate3_withOutputFn((stage1, stage2) -> srcStage.aggregate3(
                toList(), stage1, toList(), stage2, toList(), (r0, r1, r2) -> r0.size() + r1.size() + r2.size()));
    }

    @Test
    public void aggregate3_withAggrOp3_with_finishFn() {
        testAggregate3_withOutputFn((stage1, stage2) -> srcStage.aggregate3(stage1, stage2, aggregateOperation3(
                toList(), toList(), toList(), (r0, r1, r2) -> r0.size() + r1.size() + r2.size())));
    }

    private void testAggregate3_withOutputFn(
            BiFunction<BatchStage<Integer>, BatchStage<Integer>, BatchStage<Integer>> attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> stage1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> stage2 = p.drawFrom(mapValuesSource(src2Name));

        // When
        BatchStage<Integer> aggregated = attachAggregatingStageFn.apply(stage1, stage2);

        //Then
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        @SuppressWarnings("unchecked")
        int actual = (Integer) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(3 * input.size(), actual);
    }

    private class AggregateBuilderFixture {
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, String> mapFn1 = i -> i + "-a";
        DistributedFunction<Integer, String> mapFn2 = i -> i + "-b";
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        BatchStage<String> stage1 = p.drawFrom(mapValuesSource(src1Name)).map(mapFn1);
        BatchStage<String> stage2 = p.drawFrom(mapValuesSource(src2Name)).map(mapFn2);
        {
            putToBatchSrcMap(input);
            putToMap(jet().getMap(src1Name), input);
            putToMap(jet().getMap(src2Name), input);
        }
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void aggregateBuilder_withSeparateAggrOps() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        AggregateBuilder<List<Integer>> b = srcStage.aggregateBuilder(toList());
        Tag<List<Integer>> tag0 = b.tag0();
        Tag<List<String>> tag1 = b.add(fx.stage1, toList());
        Tag<List<String>> tag2 = b.add(fx.stage2, toList());
        BatchStage<ItemsByTag> aggregated = b.build();

        //Then
        validateAggrBuilder(fx, tag0, tag1, tag2, aggregated);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void aggregateBuilder_with_complexAggrOp() {
        // Given
        AggregateBuilderFixture fx = new AggregateBuilderFixture();

        // When
        AggregateBuilder1<Integer> b = srcStage.aggregateBuilder();
        Tag<Integer> tag0_in = b.tag0();
        Tag<String> tag1_in = b.add(fx.stage1);
        Tag<String> tag2_in = b.add(fx.stage2);

        CoAggregateOperationBuilder agb = coAggregateOperationBuilder();
        Tag<List<Integer>> tag0 = agb.add(tag0_in, toList());
        Tag<List<String>> tag1 = agb.add(tag1_in, toList());
        Tag<List<String>> tag2 = agb.add(tag2_in, toList());
        AggregateOperation<Object[], ItemsByTag> aggrOp = agb.build();

        BatchStage<ItemsByTag> aggregated = b.build(aggrOp);

        // Then
        validateAggrBuilder(fx, tag0, tag1, tag2, aggregated);
    }

    private void validateAggrBuilder(
            AggregateBuilderFixture fx,
            Tag<List<Integer>> tag0, Tag<List<String>> tag1, Tag<List<String>> tag2,
            BatchStage<ItemsByTag> aggregated
    ) {
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        ItemsByTag actual = (ItemsByTag) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(toBag(fx.input), toBag(requireNonNull(actual.get(tag0))));
        assertEquals(toBag(fx.input.stream().map(fx.mapFn1).collect(Collectors.toList())),
                toBag(requireNonNull(actual.get(tag1))));
        assertEquals(toBag(fx.input.stream().map(fx.mapFn2).collect(Collectors.toList())),
                toBag(requireNonNull(actual.get(tag2))));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void aggregateBuilder_withSeparateAggrOps_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> stage1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> stage2 = p.drawFrom(mapValuesSource(src2Name));

        // When
        AggregateBuilder<Long> b = srcStage.aggregateBuilder(counting());
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, counting());
        Tag<Long> tag2 = b.add(stage2, counting());
        BatchStage<Long> aggregated = b.build(ibt -> ibt.get(tag0) + ibt.get(tag1) + ibt.get(tag2));

        //Then
        validateAggBuilder_withOutputFn(input, aggregated);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void aggregateBuilder_with_complexAggrOp_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        putToBatchSrcMap(input);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> stage1 = p.drawFrom(mapValuesSource(src1Name));
        BatchStage<Integer> stage2 = p.drawFrom(mapValuesSource(src2Name));

        // When
        AggregateBuilder1<Integer> b = srcStage.aggregateBuilder();
        Tag<Integer> tag0_in = b.tag0();
        Tag<Integer> tag1_in = b.add(stage1);
        Tag<Integer> tag2_in = b.add(stage2);

        CoAggregateOperationBuilder agb = coAggregateOperationBuilder();
        Tag<Long> tag0 = agb.add(tag0_in, counting());
        Tag<Long> tag1 = agb.add(tag1_in, counting());
        Tag<Long> tag2 = agb.add(tag2_in, counting());
        AggregateOperation<Object[], Long> aggrOp = agb.build(ibt -> ibt.get(tag0) + ibt.get(tag1) + ibt.get(tag2));

        BatchStage<Long> aggregated = b.build(aggrOp);

        //Then
        validateAggBuilder_withOutputFn(input, aggregated);
    }

    private void validateAggBuilder_withOutputFn(List<Integer> input, BatchStage<Long> aggregated) {
        aggregated.drainTo(sink);
        execute();
        Iterator<?> sinkIter = sinkList.iterator();
        assertTrue(sinkIter.hasNext());
        long actual = (Long) sinkIter.next();
        assertFalse(sinkIter.hasNext());
        assertEquals(3 * input.size(), actual);
    }

    @Test
    public void groupAggregate() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i % 5;
        putToBatchSrcMap(input);

        // When
        BatchStage<Entry<Integer, Long>> aggregated = srcStage
                .groupingKey(keyFn)
                .aggregate(summingLong(i -> i));

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected = input.stream().collect(groupingBy(keyFn, Collectors.summingLong(i -> i)));
        assertEquals(toBag(expected.entrySet()), sinkToBag());
    }

    @Test
    public void groupAggregate_withOutputFn() {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i % 5;
        putToBatchSrcMap(input);

        // When
        BatchStage<Entry<Integer, Long>> aggregated = srcStage
                .groupingKey(keyFn)
                .aggregate(summingLong(i -> i), (key, sum) -> entry(key, 3 * sum));

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected = input.stream().collect(groupingBy(keyFn, Collectors.summingLong(i -> 3 * i)));
        assertEquals(toBag(expected.entrySet()), sinkToBag());
    }

    @Test
    public void groupAggregate2_withSeparateAggrOps() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate2((stage0, stage1) -> stage0.aggregate2(aggrOp, stage1, aggrOp));
    }

    @Test
    public void groupAggregate2_withAggrOp2() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate2((stage0, stage1) -> stage0.aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp)));
    }

    private void testGroupAggregate2(
            BiFunction<
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    BatchStage<Entry<Integer, Tuple2<Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStage<Entry<Integer, Tuple2<Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0, stage1);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected0 = input.stream()
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected1 = input.stream()
                                            .map(mapFn1)
                                            .collect(groupingBy(keyFn, collectOp));
        for (Object item : sinkList) {
            @SuppressWarnings("unchecked")
            Entry<Integer, Tuple2<Long, Long>> e = (Entry<Integer, Tuple2<Long, Long>>) item;
            Integer key = e.getKey();
            Tuple2<Long, Long> value = e.getValue();
            assertEquals(expected0.getOrDefault(key, 0L), value.f0());
            assertEquals(expected1.getOrDefault(key, 0L), value.f1());
        }
    }

    @Test
    public void groupAggregate2_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate2_withOutputFn((stage0, stage1, outputFn) ->
                stage0.aggregate2(aggrOp, stage1, aggrOp, outputFn));
    }

    @Test
    public void groupAggregate2_withAggrOp2_withOutputFn() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate2_withOutputFn((stage0, stage1, outputFn) ->
                stage0.aggregate2(stage1, aggregateOperation2(aggrOp, aggrOp), outputFn));
    }

    private void testGroupAggregate2_withOutputFn(
            TriFunction<
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    DistributedBiFunction<Integer, Tuple2<Long, Long>, Long>,
                    BatchStage<Long>
                > attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        long a = 37;
        DistributedBiFunction<Integer, Tuple2<Long, Long>, Long> outputFn = (k, v) -> v.f1() + a * (v.f0() + a * k);
        String src1Name = randomMapName();
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStage<Long> aggregated = attachAggregatingStageFn.apply(stage0, stage1, outputFn);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = input.stream()
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr1 = input.stream()
                                                .map(mapFn1)
                                                .collect(groupingBy(keyFn, collectOp));
        Set<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        List<Long> expectedOutput = keys
            .stream()
            .map(k -> outputFn.apply(k, tuple2(expectedAggr0.getOrDefault(k, 0L), expectedAggr1.getOrDefault(k, 0L))))
            .collect(Collectors.toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }

    @Test
    public void groupAggregate3_withSeparateAggrOps() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate3((stage0, stage1, stage2) ->
                stage0.aggregate3(aggrOp, stage1, aggrOp, stage2, aggrOp));
    }

    @Test
    public void groupAggregate3_withAggrOp3() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate3((stage0, stage1, stage2) ->
                stage0.aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp))
        );
    }

    private void testGroupAggregate3(
            TriFunction<
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    BatchStage<Entry<Integer, Tuple3<Long, Long, Long>>>>
                attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage2 = srcStage2.groupingKey(keyFn);
        BatchStage<Entry<Integer, Tuple3<Long, Long, Long>>> aggregated =
                attachAggregatingStageFn.apply(stage0, stage1, stage2);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expected0 = input.stream()
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected1 = input.stream()
                                            .map(mapFn1)
                                            .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expected2 = input.stream()
                                            .map(mapFn2)
                                            .collect(groupingBy(keyFn, collectOp));
        for (Object item : sinkList) {
            @SuppressWarnings("unchecked")
            Entry<Integer, Tuple3<Long, Long, Long>> e = (Entry<Integer, Tuple3<Long, Long, Long>>) item;
            Integer key = e.getKey();
            Tuple3<Long, Long, Long> value = e.getValue();
            assertEquals(expected0.getOrDefault(key, 0L), value.f0());
            assertEquals(expected1.getOrDefault(key, 0L), value.f1());
            assertEquals(expected2.getOrDefault(key, 0L), value.f2());
        }
    }

    @Test
    public void groupAggregate3_withSeparateAggrOps_withOutputFn() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate3_withOutputFn((stage0, stage1, stage2, outputFn) ->
                stage0.aggregate3(aggrOp, stage1, aggrOp, stage2, aggrOp, outputFn));
    }

    @Test
    public void groupAggregate3_withAggrOp3_withOutputFn() {
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        testGroupAggregate3_withOutputFn((stage0, stage1, stage2, outputFn) ->
                stage0.aggregate3(stage1, stage2, aggregateOperation3(aggrOp, aggrOp, aggrOp), outputFn)
        );
    }

    private void testGroupAggregate3_withOutputFn(
            QuadFunction<
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    BatchStageWithKey<Integer, Integer>,
                    DistributedBiFunction<Integer, Tuple3<Long, Long, Long>, Long>,
                    BatchStage<Long>
                > attachAggregatingStageFn
    ) {
        // Given
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        long a = 37;
        DistributedBiFunction<Integer, Tuple3<Long, Long, Long>, Long> outputFn = (k, v) ->
                v.f2() + a * (v.f1() + a * (v.f0() + a * k));
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        putToBatchSrcMap(input);
        putToMap(jet().getMap(src1Name), input);
        putToMap(jet().getMap(src2Name), input);
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = srcStage1.groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage2 = srcStage2.groupingKey(keyFn);
        BatchStage<Long> aggregated =
                attachAggregatingStageFn.apply(stage0, stage1, stage2, outputFn);

        //Then
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = input.stream()
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr1 = input.stream()
                                                .map(mapFn1)
                                                .collect(groupingBy(keyFn, collectOp));
        Map<Integer, Long> expectedAggr2 = input.stream()
                                                .map(mapFn2)
                                                .collect(groupingBy(keyFn, collectOp));
        Set<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        keys.addAll(expectedAggr2.keySet());
        List<Long> expectedOutput = keys
            .stream()
            .map(k -> outputFn.apply(k, tuple3(
                    expectedAggr0.getOrDefault(k, 0L),
                    expectedAggr1.getOrDefault(k, 0L),
                    expectedAggr2.getOrDefault(k, 0L)
            )))
            .collect(Collectors.toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }

    private class GroupAggregateBuilderFixture {
        List<Integer> input = sequence(itemCount);
        DistributedFunction<Integer, Integer> keyFn = i -> i / 5;
        DistributedFunction<Integer, Integer> mapFn1 = i -> 10 * i;
        DistributedFunction<Integer, Integer> mapFn2 = i -> 100 * i;
        AggregateOperation1<Integer, ?, Long> aggrOp = summingLong(i -> i);
        Collector<Integer, ?, Long> collectOp = Collectors.summingLong(i -> i);
        String src1Name = randomMapName();
        String src2Name = randomMapName();
        BatchStage<Integer> srcStage1 = p.drawFrom(mapValuesSource(src1Name))
                                         .map(mapFn1);
        BatchStage<Integer> srcStage2 = p.drawFrom(mapValuesSource(src2Name))
                                         .map(mapFn2);
        {
            putToBatchSrcMap(input);
            putToMap(jet().getMap(src1Name), input);
            putToMap(jet().getMap(src2Name), input);
        }

        @SuppressWarnings("ConstantConditions")
        DistributedBiFunction<Integer, ItemsByTag, Long> outputFn(
                Tag<Long> tag0, Tag<Long> tag1, Tag<Long> tag2
        ) {
            long a = 37;
            return (k, v) -> v.get(tag2) + a * (v.get(tag1) + a * (v.get(tag0) + a * k));
        }
    }

    @Test
    public void groupAggregateBuilder_withSeparateAggrOps_withOutputFn() {
        // Given
        GroupAggregateBuilderFixture fx = new GroupAggregateBuilderFixture();

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(fx.keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = fx.srcStage1.groupingKey(fx.keyFn);
        BatchStageWithKey<Integer, Integer> stage2 = fx.srcStage2.groupingKey(fx.keyFn);
        GroupAggregateBuilder<Integer, Long> b = stage0.aggregateBuilder(fx.aggrOp);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, fx.aggrOp);
        Tag<Long> tag2 = b.add(stage2, fx.aggrOp);
        DistributedBiFunction<Integer, ItemsByTag, Long> outputFn = fx.outputFn(tag0, tag1, tag2);
        BatchStage<Long> aggregated = b.build(outputFn);

        //Then
        validateGroupAggrBuilder(fx, tag0, tag1, tag2, outputFn, aggregated);
    }

    @Test
    public void groupAggregateBuilder_withComplexAggrOp_withOutputFn() {
        // Given
        GroupAggregateBuilderFixture fx = new GroupAggregateBuilderFixture();

        // When
        BatchStageWithKey<Integer, Integer> stage0 = srcStage.groupingKey(fx.keyFn);
        BatchStageWithKey<Integer, Integer> stage1 = fx.srcStage1.groupingKey(fx.keyFn);
        BatchStageWithKey<Integer, Integer> stage2 = fx.srcStage2.groupingKey(fx.keyFn);

        GroupAggregateBuilder1<Integer, Integer> b = stage0.aggregateBuilder();
        Tag<Integer> inTag0 = b.tag0();
        Tag<Integer> inTag1 = b.add(stage1);
        Tag<Integer> inTag2 = b.add(stage2);

        CoAggregateOperationBuilder agb = coAggregateOperationBuilder();
        Tag<Long> tag0 = agb.add(inTag0, fx.aggrOp);
        Tag<Long> tag1 = agb.add(inTag1, fx.aggrOp);
        Tag<Long> tag2 = agb.add(inTag2, fx.aggrOp);
        AggregateOperation<Object[], ItemsByTag> complexAggrOp = agb.build();
        DistributedBiFunction<Integer, ItemsByTag, Long> outputFn = fx.outputFn(tag0, tag1, tag2);
        BatchStage<Long> aggregated = b.build(complexAggrOp, outputFn);

        //Then
        validateGroupAggrBuilder(fx, tag0, tag1, tag2, outputFn, aggregated);
    }

    private void validateGroupAggrBuilder(
            GroupAggregateBuilderFixture fx,
            Tag<Long> tag0, Tag<Long> tag1, Tag<Long> tag2,
            DistributedBiFunction<Integer, ItemsByTag, Long> outputFn,
            BatchStage<Long> aggregated
    ) {
        aggregated.drainTo(sink);
        execute();
        Map<Integer, Long> expectedAggr0 = fx.input.stream()
                                                   .collect(groupingBy(fx.keyFn, fx.collectOp));
        Map<Integer, Long> expectedAggr1 = fx.input.stream()
                                                   .map(fx.mapFn1)
                                                   .collect(groupingBy(fx.keyFn, fx.collectOp));
        Map<Integer, Long> expectedAggr2 = fx.input.stream()
                                                   .map(fx.mapFn2)
                                                   .collect(groupingBy(fx.keyFn, fx.collectOp));
        Set<Integer> keys = new HashSet<>(expectedAggr0.keySet());
        keys.addAll(expectedAggr1.keySet());
        keys.addAll(expectedAggr2.keySet());
        List<Long> expectedOutput = keys
                .stream()
                .map(k -> outputFn.apply(k, itemsByTag(
                        tag0, expectedAggr0.getOrDefault(k, 0L),
                        tag1, expectedAggr1.getOrDefault(k, 0L),
                        tag2, expectedAggr2.getOrDefault(k, 0L)
                )))
                .collect(Collectors.toList());
        assertEquals(toBag(expectedOutput), sinkToBag());
    }

    interface QuadFunction<T0, T1, T2, T3, R> {
        R apply(T0 t0, T1 t1, T2 t2, T3 t3);
    }
}
