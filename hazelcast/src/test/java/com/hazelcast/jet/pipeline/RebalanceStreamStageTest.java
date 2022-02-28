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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.WindowResult;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RebalanceStreamStageTest extends PipelineStreamTestSupport {
    private static final AggregateOperation1<Integer, LongAccumulator, Long> SUMMING =
            AggregateOperations.summingLong(i -> i);

    @Test
    public void when_rebalanceAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.rebalance().map(formatFn);

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
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.rebalance(i -> i).map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNotNull("Rebalanced by key, the edge must be partitioned", srcToMap.getPartitioner());
    }

    @Test
    public void when_peekAndRebalanceAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.peek().rebalance().map(formatFn);

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
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.peek().rebalance(i -> i).map(formatFn);

        // Then
        mapped.writeTo(sink);
        DAG dag = p.toDag();
        Edge srcToMap = dag.getInboundEdges("map").get(0);
        assertTrue("Rebalancing should make the edge distributed", srcToMap.isDistributed());
        assertNotNull("Rebalanced by key, the edge must be partitioned", srcToMap.getPartitioner());
    }

    @Test(expected = JetException.class)
    public void when_rebalanceAndPeekAndMap_then_dagEdgeDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.rebalance().peek().map(formatFn);
    }

    @Test(expected = JetException.class)
    public void when_rebalanceByKeyAndPeekAndMap_then_dagEdgePartitionedDistributed() {
        // Given
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, String> formatFn = i -> String.format("%04d-string", i);

        // When
        StreamStage<String> mapped = srcStage.rebalance(i -> i).peek().map(formatFn);
    }

    @Test
    public void when_rebalanceAndWindowAggregate_then_unicastDistributedEdgeAndTwoStageAggregation() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);

        // When
        StreamStage<WindowResult<Long>> aggregated = srcStage
                .rebalance()
                .window(tumbling(1))
                .aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertTwoStageAggregation(false);
    }

    @Test
    public void when_rebalanceByKeyAndWindowAggregate_then_distributedPartitionedEdgeAndTwoStageAggregation() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);

        // When
        StreamStage<WindowResult<Long>> aggregated = srcStage
                .rebalance(i -> 2 * i)
                .window(tumbling(1))
                .aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertTwoStageAggregation(true);
    }

    @Test
    public void when_rebalanceAndWindowGroupAggregate_then_singleStageAggregation() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, Integer> keyFn = i -> i % 5;

        // When
        StreamStage<KeyedWindowResult<Integer, Long>> aggregated = srcStage
                .rebalance()
                .window(tumbling(1))
                .groupingKey(keyFn)
                .aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
    }

    @Test
    public void when_rebalanceByKeyAndWindowGroupAggregate_then_singleStageAggregation() {
        List<Integer> input = sequence(itemCount);
        StreamStage<Integer> srcStage = streamStageFromList(input);
        FunctionEx<Integer, Integer> rebalanceFn = i -> i % 3;
        FunctionEx<Integer, Integer> keyFn = i -> i % 5;

        // When
        StreamStage<KeyedWindowResult<Integer, Long>> aggregated = srcStage
                .rebalance(rebalanceFn)
                .window(tumbling(1))
                .groupingKey(keyFn)
                .aggregate(SUMMING);

        // Then
        aggregated.writeTo(sink);
        assertSingleStageAggregation();
    }

    private void assertSingleStageAggregation() {
        DAG dag = p.toDag();
        try {
            Edge srcToAggregate = dag.getOutboundEdges("add-timestamps").get(0);
            assertTrue("Outbound edge after rebalancing must be distributed", srcToAggregate.isDistributed());
            Edge aggregateToSink = dag.getOutboundEdges(srcToAggregate.getDestName()).get(0);
            List<Edge> sinkOutbound = dag.getOutboundEdges(aggregateToSink.getDestName());
            assertEquals("Aggregation after rebalancing must be single-stage", 0, sinkOutbound.size());
        } catch (AssertionError e) {
            System.err.println(dag.toDotString());
            throw e;
        }
    }

    private void assertTwoStageAggregation(boolean isRebalanceByKey) {
        DAG dag = p.toDag();
        try {
            Edge srcToAggregate = dag.getOutboundEdges("add-timestamps").get(0);
            assertEquals(
                    isRebalanceByKey
                            ? "Edge to aggregation after rebalancing by key must be partitioned"
                            : "Rebalanced edge to global aggregation must be unicast",
                    isRebalanceByKey, srcToAggregate.getPartitioner() != null);
            assertTrue("Outbound edge after rebalancing must be distributed", srcToAggregate.isDistributed());
            String accumulatingVertexName = srcToAggregate.getDestName();
            String aggregatePrepare = "sliding-window-prepare";
            assertEquals("Aggregation must be two-stage;",
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
