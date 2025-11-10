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

import com.hazelcast.collection.IList;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.BatchAggregateTest.FORMAT_FN;
import static com.hazelcast.jet.pipeline.BatchAggregateTest.FORMAT_FN_2;
import static com.hazelcast.jet.pipeline.BatchAggregateTest.FORMAT_FN_3;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static java.util.Collections.singletonList;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class RebalanceBatchStageTest extends PipelineTestSupport {

    private static final AggregateOperation1<Integer, LongAccumulator, Long> SUMMING =
            AggregateOperations.summingLong(i -> i);

    @Test
    public void when_rebalanceAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.rebalance().map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", srcToMap.getPartitioner());
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void when_rebalanceByKeyAndMap_then_dagEdgePartitionedDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.rebalance(i -> i).map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNotNull("Rebalancing by key, the edge must be partitioned", srcToMap.getPartitioner());
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void when_peekAndRebalanceAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.peek().rebalance().map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", srcToMap.getPartitioner());
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test
    public void when_peekAndRebalanceByKeyAndMap_then_dagEdgePartitionedDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.peek().rebalance(i -> i).map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNotNull("Rebalancing by key, the edge must be partitioned", srcToMap.getPartitioner());
        execute();
        assertEquals(streamToString(input.stream(), formatFn),
                streamToString(sinkStreamOf(String.class), identity()));
    }

    @Test(expected = JetException.class)
    public void when_rebalanceAndPeekAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.rebalance().peek().map(formatFn);
    }

    @Test(expected = JetException.class)
    public void when_rebalanceByKeyAndPeekAndMap_then_dagEdgePartitionedDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        BatchStage<String> mapped = srcStage.rebalance(i -> i).peek().map(formatFn);
    }

    @Test
    public void when_mergeWithRebalanced_thenOnlyRebalancedEdgeDistributed() {
        // Given
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStage<Integer> stage0 = batchStageFromList(input0);
        BatchStage<Integer> stage1 = batchStageFromList(input1).rebalance();

        // When
        BatchStage<Integer> merged = stage0.merge(stage1);

        // Then
        merged.writeTo(assertAnyOrder(IntStream.range(0, 2 * itemCount).boxed().collect(toList())));

        DAG dag = p.toDag();
        List<Edge> intoMerge = dag.getInboundEdges("merge");
        assertFalse("Didn't rebalance this stage, why is its edge distributed?", intoMerge.get(0).isDistributed());
        assertTrue("Rebalancing should make the edge distributed", intoMerge.get(1).isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", intoMerge.get(1).getPartitioner());

        execute();
    }

    @Test
    public void when_mergeWithRebalancedByKey_thenOnlyRebalancedEdgePartitionedDistributed() {
        // Given
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStage<Integer> stage0 = batchStageFromList(input0);
        BatchStage<Integer> stage1 = batchStageFromList(input1).rebalance(i -> i);

        // When
        BatchStage<Integer> merged = stage0.merge(stage1);

        // Then
        merged.writeTo(assertAnyOrder(IntStream.range(0, 2 * itemCount).boxed().collect(toList())));

        DAG dag = p.toDag();
        List<Edge> intoMerge = dag.getInboundEdges("merge");
        assertFalse("Didn't rebalance this stage, why is its edge distributed?", intoMerge.get(0).isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", intoMerge.get(0).getPartitioner());
        assertTrue("Rebalancing should make the edge distributed", intoMerge.get(1).isDistributed());
        assertNotNull("Rebalancing by key, the edge must be partitioned", intoMerge.get(1).getPartitioner());

        execute();
    }

    @Test
    public void when_mergeRebalancedWithNonRebalanced_thenOnlyRebalancedEdgeDistributed() {
        // Given
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStage<Integer> stage0 = batchStageFromList(input0).rebalance();
        BatchStage<Integer> stage1 = batchStageFromList(input1);

        // When
        BatchStage<Integer> merged = stage0.merge(stage1);

        // Then
        merged.writeTo(assertAnyOrder(IntStream.range(0, 2 * itemCount).boxed().collect(toList())));

        DAG dag = p.toDag();
        List<Edge> intoMerge = dag.getInboundEdges("merge");
        assertTrue("Rebalancing should make the edge distributed", intoMerge.get(0).isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", intoMerge.get(0).getPartitioner());
        assertFalse("Didn't rebalance this stage, why is its edge distributed?", intoMerge.get(1).isDistributed());

        execute();
    }

    @Test
    public void when_hashJoinRebalanceMainStage_then_distributedEdge() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Integer> stage0Rebalanced = batchStageFromList(input).rebalance();
        BatchStage<Entry<Integer, String>> enrichingStage =
                batchStageFromList(input).map(i -> entry(i, prefix + i));

        // When
        BatchStage<Entry<Integer, String>> joined = stage0Rebalanced.hashJoin(
                enrichingStage,
                joinMapEntries(wholeItem()),
                // Method reference avoided due to JDK bug
                Util::entry);

        // Then
        joined.writeTo(sink);
        DAG dag = p.toDag();
        Edge stage0ToJoin = dag.getInboundEdges("2-way hash-join-joiner").get(0);
        assertTrue("Rebalancing should make the edge distributed", stage0ToJoin.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", stage0ToJoin.getPartitioner());
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));

    }

    @Test
    public void when_hashJoinRebalanceEnrichingStage_then_noEffect() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Integer> mainStage = batchStageFromList(input);
        BatchStage<Entry<Integer, String>> enrichingStageRebalanced = batchStageFromList(input)
                .map(i -> entry(i, prefix + i))
                .rebalance();

        // When
        BatchStage<Entry<Integer, String>> joined = mainStage.hashJoin(
                enrichingStageRebalanced,
                joinMapEntries(wholeItem()),
                Util::entry);

        // Then
        joined.writeTo(sink);
        DAG dag = p.toDag();
        Edge mapToJoin = dag.getInboundEdges("2-way hash-join-collector1").get(0);
        assertTrue("Edge into a hash-join collector vertex must be distributed", mapToJoin.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", mapToJoin.getPartitioner());
        Edge stage0ToJoin = dag.getInboundEdges("2-way hash-join-joiner").get(0);
        assertFalse("Didn't rebalance this stage, why is its edge distributed?", stage0ToJoin.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", stage0ToJoin.getPartitioner());
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(sinkStreamOfEntry(), formatFn));

    }

    @Test
    public void when_hashJoinBuilderRebalanceMainStage_then_distributedEdge() {
        // Given
        List<Integer> input = sequence(itemCount);
        String prefix = "value-";
        BatchStage<Integer> stage0Rebalanced = batchStageFromList(input).rebalance();
        BatchStage<Entry<Integer, String>> enrichingStage =
                batchStageFromList(input).map(i -> entry(i, prefix + i));

        // When
        HashJoinBuilder<Integer> b = stage0Rebalanced.hashJoinBuilder();
        Tag<String> tag1 = b.add(enrichingStage, joinMapEntries(wholeItem()));
        // Method reference avoided due to JDK bug
        BatchStage<Entry<Integer, ItemsByTag>> joined = b.build((k, v) -> entry(k, v));

        // Then
        joined.writeTo(sink);
        DAG dag = p.toDag();
        Edge stage0ToJoin = dag.getInboundEdges("2-way hash-join-joiner").get(0);
        assertTrue("Rebalancing should make the edge distributed", stage0ToJoin.isDistributed());
        assertNull("Didn't rebalance by key, the edge must not be partitioned", stage0ToJoin.getPartitioner());
        execute();
        Function<Entry<Integer, String>, String> formatFn =
                e -> String.format("(%04d, %s)", e.getKey(), e.getValue());
        assertEquals(
                streamToString(input.stream().map(i -> tuple2(i, prefix + i)), formatFn),
                streamToString(this.<Integer, ItemsByTag>sinkStreamOfEntry(), e ->
                        formatFn.apply(entry(e.getKey(), e.getValue().get(tag1)))));

    }

    @Test
    public void when_rebalanceAndAggregate_then_unicastDistributedEdgeAndTwoStageAggregation() {
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);

        // When
        BatchStage<Long> aggregated = srcStage
                .rebalance()
                .aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertTwoStageGlobalAggregation(1);
        execute();
        assertEquals(
                singletonList(input.stream().mapToLong(i -> i).sum()),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void when_rebalanceByKeyAndAggregate_then_distributedPartitionedEdgeAndTwoStageAggregation() {
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage = batchStageFromList(input);

        // When
        BatchStage<Long> aggregated = srcStage.rebalance(i -> 2 * i).aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        DAG dag = p.toDag();
        try {
            Edge srcToAggregate = dag.getOutboundEdges("items").get(0);
            assertNotNull("Rebalanced edge to grouped aggregation must be partitioned", srcToAggregate.getPartitioner());
            assertTrue("Outbound edge after rebalancing must be distributed", srcToAggregate.isDistributed());
            String accumulatingVertexName = srcToAggregate.getDestName();
            assertEquals("Aggregation must be two-stage", "aggregate-prepare", accumulatingVertexName);
            Edge internalAggregationEdge = dag.getOutboundEdges(accumulatingVertexName).get(0);
            assertTrue("Internal aggregation edge must be distributed", internalAggregationEdge.isDistributed());
            assertNotNull("Internal aggregation edge must be partitioned", internalAggregationEdge.getPartitioner());
        } catch (AssertionError e) {
            System.err.println(dag.toDotString());
            throw e;
        }
        execute();
        assertEquals(
                singletonList(input.stream().mapToLong(i -> i).sum()),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void when_aggregate2WithRebalancedStage_then_twoStageAggregation() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage1 = batchStageFromList(input);
        BatchStage<Integer> srcStage2 = batchStageFromList(input);

        // When
        BatchStage<Tuple2<Long, Long>> aggregated = srcStage1
                .aggregate2(SUMMING,
                        srcStage2.rebalance(), SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertTwoStageGlobalAggregation(2);
        execute();
        long expectedSum = input.stream().mapToLong(i -> i).sum();
        assertEquals(
                singletonList(tuple2(expectedSum, expectedSum)),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void when_aggregate3WithRebalancedStage_then_twoStageAggregation() {
        // Given
        List<Integer> input = sequence(itemCount);
        BatchStage<Integer> srcStage1 = batchStageFromList(input);
        BatchStage<Integer> srcStage2 = batchStageFromList(input);
        BatchStage<Integer> srcStage3 = batchStageFromList(input);

        // When
        BatchStage<Tuple3<Long, Long, Long>> aggregated = srcStage1
                .aggregate3(SUMMING,
                        srcStage2, SUMMING,
                        srcStage3.rebalance(), SUMMING
                );

        // Then
        aggregated.writeTo(sink);
        assertTwoStageGlobalAggregation(3);
        execute();
        long expectedSum = input.stream().mapToLong(i -> i).sum();
        assertEquals(
                singletonList(tuple3(expectedSum, expectedSum, expectedSum)),
                new ArrayList<>(sinkList)
        );
    }

    @Test
    public void when_rebalanceAndGroupAggregate_then_singleStageAggregation() {
        // Given
        List<Integer> input = sequence(itemCount);
        FunctionEx<Integer, Integer> keyFn = i -> i % 5;

        // When
        BatchStage<Entry<Integer, Long>> aggregated = batchStageFromList(input)
                .rebalance()
                .groupingKey(keyFn)
                .aggregate(SUMMING);

        // Then
        Map<Integer, Long> expected = input.stream().collect(groupingBy(keyFn, summingLong(i -> i)));
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
        execute();
        assertEquals(
                streamToString(expected.entrySet().stream(), FORMAT_FN),
                streamToString(sinkStreamOfEntry(), FORMAT_FN));
    }

    @Test
    public void when_rebalanceAndGroupAggregate2_then_singleStageAggregation() {
        // Given
        FunctionEx<Integer, Integer> keyFn = i -> i % 10;
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStageWithKey<Integer, Integer> stage0 =
                batchStageFromList(input0).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1Rebalanced =
                batchStageFromList(input1).rebalance().groupingKey(keyFn);

        // When
        BatchStage<Entry<Integer, Tuple2<Long, Long>>> aggregated = stage0
                .aggregate2(SUMMING,
                        stage1Rebalanced, SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
        execute();
        Map<Integer, Long> expectedMap0 = input0.stream().collect(groupingBy(keyFn, summingLong(i -> i)));
        Map<Integer, Long> expectedMap1 = input1.stream().collect(groupingBy(keyFn, summingLong(i -> i)));
        assertEquals(
                streamToString(expectedMap0.entrySet().stream(),
                        e -> FORMAT_FN_2.apply(e.getKey(), tuple2(e.getValue(), expectedMap1.get(e.getKey())))),
                streamToString(this.<Integer, Tuple2<Long, Long>>sinkStreamOfEntry(),
                        e -> FORMAT_FN_2.apply(e.getKey(), e.getValue()))
        );
    }

    @Test
    public void when_rebalanceAndGroupAggregate3_then_singleStageAggregation() {
        // Given
        FunctionEx<Integer, Integer> keyFn = i -> i % 10;
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input2 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStageWithKey<Integer, Integer> stage0 =
                batchStageFromList(input0).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 =
                batchStageFromList(input1).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage2Rebalanced =
                batchStageFromList(input2).rebalance().groupingKey(keyFn);

        // When
        BatchStage<Entry<Integer, Tuple3<Long, Long, Long>>> aggregated = stage0
                .aggregate3(SUMMING,
                        stage1, SUMMING,
                        stage2Rebalanced, SUMMING
                );

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
        execute();
        Collector<Integer, ?, Map<Integer, Long>> groupAndSum = groupingBy(keyFn, summingLong(i -> i));
        Map<Integer, Long> expectedMap0 = input0.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap1 = input1.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap2 = input2.stream().collect(groupAndSum);
        assertEquals(
                streamToString(expectedMap0.entrySet().stream(),
                        e -> FORMAT_FN_3.apply(e.getKey(), tuple3(
                                e.getValue(), expectedMap1.get(e.getKey()), expectedMap2.get(e.getKey())))),
                streamToString(this.<Integer, Tuple3<Long, Long, Long>>sinkStreamOfEntry(),
                        e -> FORMAT_FN_3.apply(e.getKey(), e.getValue()))
        );
    }

    @Test
    public void when_rebalanceAndGroupAggregateBuilder_then_singleStageAggregation() {
        // Given
        FunctionEx<Integer, Integer> keyFn = i -> i % 10;
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input2 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStageWithKey<Integer, Integer> stage0 =
                batchStageFromList(input0).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 =
                batchStageFromList(input1).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage2Rebalanced =
                batchStageFromList(input2).rebalance().groupingKey(keyFn);

        // When
        GroupAggregateBuilder<Integer, Long> b = stage0.aggregateBuilder(SUMMING);
        Tag<Long> tag0 = b.tag0();
        Tag<Long> tag1 = b.add(stage1, SUMMING);
        Tag<Long> tag2 = b.add(stage2Rebalanced, SUMMING);

        BatchStage<Entry<Integer, ItemsByTag>> aggregated = b.build();

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
        execute();
        Collector<Integer, ?, Map<Integer, Long>> groupAndSum = groupingBy(keyFn, summingLong(i -> i));
        Map<Integer, Long> expectedMap0 = input0.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap1 = input1.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap2 = input2.stream().collect(groupAndSum);
        assertEquals(
                streamToString(expectedMap0.entrySet().stream(), e -> FORMAT_FN_3.apply(
                        e.getKey(),
                        tuple3(e.getValue(), expectedMap1.get(e.getKey()), expectedMap2.get(e.getKey())))),
                streamToString(this.<Integer, ItemsByTag>sinkStreamOfEntry(), e -> FORMAT_FN_3.apply(
                        e.getKey(),
                        tuple3(e.getValue().get(tag0), e.getValue().get(tag1), e.getValue().get(tag2))))
        );
    }

    @Test
    public void when_rebalanceAndGroupAggregateBuilderWithComplexAggrOp_then_singleStageAggregation() {
        // Given
        FunctionEx<Integer, Integer> keyFn = i -> i % 10;
        Iterator<Integer> sequence = IntStream.iterate(0, i -> i + 1).boxed().iterator();
        List<Integer> input0 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input1 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        List<Integer> input2 = streamFromIterator(sequence).limit(itemCount).collect(toList());
        BatchStageWithKey<Integer, Integer> stage0 =
                batchStageFromList(input0).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage1 =
                batchStageFromList(input1).groupingKey(keyFn);
        BatchStageWithKey<Integer, Integer> stage2Rebalanced =
                batchStageFromList(input2).rebalance().groupingKey(keyFn);

        // When
        GroupAggregateBuilder1<Integer, Integer> b = stage0.aggregateBuilder();
        Tag<Integer> tag0_in = b.tag0();
        Tag<Integer> tag1_in = b.add(stage1);
        Tag<Integer> tag2_in = b.add(stage2Rebalanced);

        CoAggregateOperationBuilder agb = coAggregateOperationBuilder();
        Tag<Long> tag0 = agb.add(tag0_in, SUMMING);
        Tag<Long> tag1 = agb.add(tag1_in, SUMMING);
        Tag<Long> tag2 = agb.add(tag2_in, SUMMING);
        AggregateOperation<Object[], ItemsByTag> aggrOp = agb.build();

        BatchStage<Entry<Integer, ItemsByTag>> aggregated = b.build(aggrOp);

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
        execute();
        Collector<Integer, ?, Map<Integer, Long>> groupAndSum = groupingBy(keyFn, summingLong(i -> i));
        Map<Integer, Long> expectedMap0 = input0.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap1 = input1.stream().collect(groupAndSum);
        Map<Integer, Long> expectedMap2 = input2.stream().collect(groupAndSum);
        assertEquals(
                streamToString(expectedMap0.entrySet().stream(), e -> FORMAT_FN_3.apply(
                        e.getKey(),
                        tuple3(e.getValue(), expectedMap1.get(e.getKey()), expectedMap2.get(e.getKey())))),
                streamToString(this.<Integer, ItemsByTag>sinkStreamOfEntry(), e -> FORMAT_FN_3.apply(
                        e.getKey(),
                        tuple3(e.getValue().get(tag0), e.getValue().get(tag1), e.getValue().get(tag2))))
        );
    }

    @Test
    public void twoConsecutiveRebalance() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3, 4, 5, 6, 7, 8))
                .rebalance()
                .filter(simpleEvent -> true)
                .setName("filter trues 1")
                .filter(simpleEvent -> true)
                .setName("filter trues 2")
                .writeTo(SinkBuilder.sinkBuilder("sink",
                        context -> context.hazelcastInstance().getList("result" + context.globalProcessorIndex()))
                .receiveFn(List::add).build());

        member.getJet().newJob(p).join();

        IList<Object> result0 = member.getList("result0");
        IList<Object> result1 = member.getList("result1");

        assertThat(result0).hasSize(4);
        assertThat(result1).hasSize(4);

    }

    @Nonnull
    private static Stream<Integer> streamFromIterator(Iterator<Integer> iter) {
        return stream(spliteratorUnknownSize(iter, 0), false);
    }

    private void assertSingleStageAggregation() {
        DAG dag =  p.toDag();
        try {
            Edge srcToAggregate = dag.getOutboundEdges("items").get(0);
            assertTrue("Outbound edge after rebalancing must be distributed", srcToAggregate.isDistributed());
            Edge aggregateToSink = dag.getOutboundEdges(srcToAggregate.getDestName()).get(0);
            List<Edge> sinkOutbound = dag.getOutboundEdges(aggregateToSink.getDestName());
            assertEquals("Aggregation after rebalancing must be single-stage", 0, sinkOutbound.size());
        } catch (AssertionError e) {
            System.err.println(dag.toDotString());
            throw e;
        }
    }

    private void assertTwoStageGlobalAggregation(int i) {
        DAG dag =  p.toDag();
        try {
            Edge srcToAggregate = dag.getOutboundEdges("items" + (i == 1 ? "" : "-" + i)).get(0);
            assertNull("Rebalanced edge to global aggregation must be unicast", srcToAggregate.getPartitioner());
            assertTrue("Outbound edge after rebalancing must be distributed", srcToAggregate.isDistributed());
            String accumulatingVertexName = srcToAggregate.getDestName();
            String aggregatePrepare = "aggregate-prepare";
            assertEquals("Aggregation must be two-stage",
                    aggregatePrepare,
                    accumulatingVertexName.substring(accumulatingVertexName.length() - aggregatePrepare.length()));
            Edge internalAggregationEdge = dag.getOutboundEdges(accumulatingVertexName).get(0);
            assertTrue("Internal aggregation edge must be distributed", internalAggregationEdge.isDistributed());
            assertNotNull("Internal aggregation edge must be partitioned", internalAggregationEdge.getPartitioner());
        } catch (AssertionError e) {
            System.err.println(dag.toDotString());
            throw e;
        }
    }
}
