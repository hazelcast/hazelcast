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

package com.hazelcast.jet.pipeline.impl;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.processor.HashJoinCollectP;
import com.hazelcast.jet.impl.processor.HashJoinP;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Stage;
import com.hazelcast.jet.pipeline.Transform;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.FilterTransform;
import com.hazelcast.jet.pipeline.impl.transform.FlatMapTransform;
import com.hazelcast.jet.pipeline.impl.transform.GroupByTransform;
import com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.ProcessorTransform;
import com.hazelcast.jet.processor.Processors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.TopologicalSorter.topologicalSort;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
class Planner {

    private static final int RANDOM_SUFFIX_LENGTH = 8;

    private final PipelineImpl pipeline;
    private final DAG dag = new DAG();
    private final Map<Stage, PlannerVertex> stage2vertex = new HashMap<>();


    Planner(PipelineImpl pipeline) {
        this.pipeline = pipeline;
    }

    DAG createDag() {
        Iterable<AbstractStage> sorted = (Iterable<AbstractStage>) (Iterable<? extends Stage>)
                topologicalSort(pipeline.adjacencyMap, Object::toString);
        for (AbstractStage stage : sorted) {
            Transform transform = stage.transform;
            if (transform instanceof SourceImpl) {
                handleSource(stage, (SourceImpl) transform);
            } else if (transform instanceof ProcessorTransform) {
                handleProcessorStage(stage, (ProcessorTransform) transform);
            } else if (transform instanceof FilterTransform) {
                handleFilter(stage, (FilterTransform) transform);
            } else if (transform instanceof MapTransform) {
                handleMap(stage, (MapTransform) transform);
            } else if (transform instanceof FlatMapTransform) {
                handleFlatMap(stage, (FlatMapTransform) transform);
            } else if (transform instanceof GroupByTransform) {
                handleGroupBy(stage, (GroupByTransform) transform);
            } else if (transform instanceof CoGroupTransform) {
                handleCoGroup(stage, (CoGroupTransform) transform);
            } else if (transform instanceof HashJoinTransform) {
                handleHashJoin(stage, (HashJoinTransform) transform);
            } else if (transform instanceof SinkImpl) {
                handleSink(stage, (SinkImpl) transform);
            } else {
                throw new IllegalArgumentException("Unknown transform " + transform);
            }
        }
        return dag;
    }

    private void handleSource(AbstractStage stage, SourceImpl source) {
        addVertex(stage, source.name(), source.metaSupplier(), false);
    }

    private void handleProcessorStage(AbstractStage stage, ProcessorTransform procTransform) {
        PlannerVertex pv = addVertex(stage,
                procTransform.transformName + '.' + randomSuffix(), procTransform.procSupplier);
        addEdges(stage, pv.v);
    }

    private void handleMap(AbstractStage stage, MapTransform map) {
        PlannerVertex pv = addVertex(stage, "map." + randomSuffix(), Processors.map(map.mapF));
        addEdges(stage, pv.v);
    }

    private void handleFilter(AbstractStage stage, FilterTransform filter) {
        PlannerVertex pv = addVertex(stage, "filter." + randomSuffix(), Processors.filter(filter.filterF));
        addEdges(stage, pv.v);
    }

    private void handleFlatMap(AbstractStage stage, FlatMapTransform flatMap) {
        PlannerVertex pv = addVertex(stage, "flatMap." + randomSuffix(),
                Processors.flatMap(flatMap.flatMapF()));
        addEdges(stage, pv.v);
    }

    private void handleGroupBy(AbstractStage stage, GroupByTransform<Object, Object, Object> groupBy) {
        String name = "groupByKey." + randomSuffix() + ".stage";
        Vertex v1 = dag.newVertex(name + '1',
                Processors.accumulateByKey(groupBy.keyF(), groupBy.aggregateOperation()));
        PlannerVertex pv2 = addVertex(stage, name + '2',
                Processors.combineByKey(groupBy.aggregateOperation()));
        addEdges(stage, v1, e -> e.partitioned(groupBy.keyF(), HASH_CODE));
        dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    private void handleCoGroup(AbstractStage stage, CoGroupTransform<Object, Object, Object> coGroup) {
        List<DistributedFunction<?, ?>> groupKeyFs = coGroup.groupKeyFs();
        String name = "coGroup." + randomSuffix() + ".stage";
        Vertex v1 = dag.newVertex(name + '1',
                Processors.coAccumulateByKey(groupKeyFs, coGroup.aggregateOperation()));
        PlannerVertex pv2 = addVertex(stage, name + '2',
                Processors.combineByKey(coGroup.aggregateOperation()));
        addEdges(stage, v1, (e, ord) -> e.partitioned(groupKeyFs.get(ord), HASH_CODE));
        dag.edge(between(v1, pv2.v).distributed().partitioned(entryKey()));
    }

    //         ---------           ----------           ----------
    //        | primary |         | joined-1 |         | joined-2 |
    //         ---------           ----------           ----------
    //             |                   |                     |
    //             |              distributed          distributed
    //             |               broadcast            broadcast
    //             |                   v                     v
    //             |             -------------         -------------
    //             |            | collector-1 |       | collector-2 |
    //             |             -------------         -------------
    //             |                   |                     |
    //             |                 local                 local
    //        distributed          broadcast             broadcast
    //        partitioned         prioritized           prioritized
    //         ordinal 0           ordinal 1             ordinal 2
    //             \                   |                     |
    //              ----------------\  |   /----------------/
    //                              v  v  v
    //                              --------
    //                             | joiner |
    //                              --------
    private void handleHashJoin(AbstractStage stage, HashJoinTransform<?> hashJoin) {
        String hashJoinName = "hashJoin." + randomSuffix();
        PlannerVertex primary = stage2vertex.get(stage.upstream.get(0));
        List<Function<Object, Object>> keyFns = (List<Function<Object, Object>>) (List)
                hashJoin.clauses().stream()
                        .map(JoinClause::leftKeyFn)
                        .collect(toList());
        Vertex joiner = addVertex(stage, hashJoinName + ".joiner",
                () -> new HashJoinP<>(keyFns, hashJoin.tags())).v;
        dag.edge(from(primary.v, primary.availableOrdinal++).to(joiner, 0));

        String collectorName = hashJoinName + ".collector.";
        int collectorOrdinal = 1;
        for (Stage fromStage : tailList(stage.upstream)) {
            PlannerVertex fromPv = stage2vertex.get(fromStage);
            JoinClause<?, ?, ?, ?> clause = hashJoin.clauses().get(collectorOrdinal - 1);
            DistributedFunction<Object, Object> getKeyF =
                    (DistributedFunction<Object, Object>) clause.rightKeyFn();
            DistributedFunction<Object, Object> projectF =
                    (DistributedFunction<Object, Object>) clause.rightProjectFn();
            Vertex collector = dag.newVertex(collectorName + collectorOrdinal,
                    () -> new HashJoinCollectP(getKeyF, projectF));
            collector.localParallelism(1);
            dag.edge(from(fromPv.v, fromPv.availableOrdinal++)
                    .to(collector, 0)
                    .distributed().broadcast());
            dag.edge(from(collector, 0)
                    .to(joiner, collectorOrdinal)
                    .broadcast().priority(-1));
            collectorOrdinal++;
        }
    }

    private void handleSink(AbstractStage stage, SinkImpl sink) {
        PlannerVertex pv = addVertex(stage, sink.name(), sink.metaSupplier(), false);
        addEdges(stage, pv.v);
    }

    private PlannerVertex addVertex(Stage stage, String name, DistributedSupplier<Processor> procSupplier) {
        return addVertex(stage, name, ProcessorMetaSupplier.of(procSupplier), true);
    }

    private PlannerVertex addVertex(
            Stage stage, String name, ProcessorMetaSupplier metaSupplier, boolean parallellize
    ) {
        Vertex v = dag.newVertex(name, metaSupplier).localParallelism(parallellize ? -1 : 1);
        PlannerVertex pv = new PlannerVertex(v);
        stage2vertex.put(stage, pv);
        return pv;
    }

    private void addEdges(AbstractStage stage, Vertex toVertex, BiConsumer<Edge, Integer> configureEdgeF) {
        int destOrdinal = 0;
        for (Stage fromStage : stage.upstream) {
            PlannerVertex fromPv = stage2vertex.get(fromStage);
            Edge edge = from(fromPv.v, fromPv.availableOrdinal++).to(toVertex, destOrdinal);
            dag.edge(edge);
            configureEdgeF.accept(edge, destOrdinal);
            destOrdinal++;
        }
    }

    private void addEdges(AbstractStage stage, Vertex toVertex, Consumer<Edge> configureEdgeF) {
        addEdges(stage, toVertex, (e, ord) -> configureEdgeF.accept(e));
    }

    private void addEdges(AbstractStage stage, Vertex toVertex) {
        addEdges(stage, toVertex, e -> { });
    }

    private static String randomSuffix() {
        String uuid = newUnsecureUUID().toString();
        return uuid.substring(uuid.length() - RANDOM_SUFFIX_LENGTH, uuid.length());
    }

    private static <E> List<E> tailList(List<E> list) {
        return list.subList(1, list.size());
    }

    private static class PlannerVertex {
        Vertex v;

        int availableOrdinal;

        PlannerVertex(Vertex v) {
            this.v = v;
        }

        @Override
        public String toString() {
            return v.toString();
        }
    }
}
