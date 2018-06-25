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

import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.pipeline.ComputeStageImplBase;
import com.hazelcast.jet.impl.pipeline.FunctionAdapter;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.pipeline.AbstractStage.transformOf;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

/**
 * Offers a step-by-step fluent API to build a hash-join pipeline stage.
 * To obtain it, call {@link GeneralStage#hashJoinBuilder()} on the primary
 * stage, the one whose data will be enriched from all other stages.
 * <p>
 * Collect all the tags returned from {@code add()} and use them to retrieve
 * the enriching items from {@link ItemsByTag} you get in the result.
 * <p>
 * This object is mainly intended to build a hash-join of the primary stage
 * with three or more contributing stages. For one or two stages, prefer the
 * direct {@code stage.hashJoin(...)} calls because they offer more static
 * type safety.
 *
 * @param <T0> the type of the items in the primary stage
 */
public abstract class GeneralHashJoinBuilder<T0> {
    private final Transform transform0;
    private final PipelineImpl pipelineImpl;
    private final FunctionAdapter fnAdapter;
    private final CreateOutStageFn<T0> createOutStageFn;
    private final Map<Tag<?>, TransformAndClause> clauses = new HashMap<>();

    GeneralHashJoinBuilder(GeneralStage<T0> stage0, CreateOutStageFn<T0> createOutStageFn) {
        this.transform0 = transformOf(stage0);
        this.pipelineImpl = (PipelineImpl) stage0.getPipeline();
        this.createOutStageFn = createOutStageFn;
        this.fnAdapter = ((ComputeStageImplBase) stage0).fnAdapter;
    }

    /**
     * Adds another contributing pipeline stage to the hash-join operation.
     *
     * @param stage the contributing stage
     * @param joinClause specifies how to join the contributing stage
     * @param <K> the type of the join key
     * @param <T1_IN> the type of the contributing stage's data
     * @param <T1> the type of result after applying the projecting transformation
     *             to the contributing stage's data
     * @return the tag that refers to the contributing stage
     */
    public <K, T1_IN, T1> Tag<T1> add(BatchStage<T1_IN> stage, JoinClause<K, T0, T1_IN, T1> joinClause) {
        Tag<T1> tag = tag(clauses.size());
        clauses.put(tag, new TransformAndClause<>(stage, joinClause));
        return tag;
    }

    @SuppressWarnings("unchecked")
    <R> GeneralStage<R> build0(DistributedBiFunction<T0, ItemsByTag, R> mapToOutputFn) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        List<Entry<Tag<?>, TransformAndClause>> orderedClauses = clauses.entrySet().stream()
                                                                        .sorted(comparing(Entry::getKey))
                                                                        .collect(toList());
        List<Transform> upstream =
                concat(
                        Stream.of(transform0),
                        orderedClauses.stream().map(e -> e.getValue().transform())
                ).collect(toList());
        // A probable javac bug forced us to extract this variable
        Stream<JoinClause<?, T0, ?, ?>> joinClauses = orderedClauses
                .stream()
                .map(e -> e.getValue().clause())
                .map(fnAdapter::adaptJoinClause);
        HashJoinTransform<T0, R> hashJoinTransform = new HashJoinTransform<>(
                upstream,
                joinClauses.collect(toList()),
                orderedClauses.stream()
                              .map(Entry::getKey)
                              .collect(toList()),
                fnAdapter.adaptHashJoinOutputFn(mapToOutputFn));
        pipelineImpl.connect(upstream, hashJoinTransform);
        return createOutStageFn.get(hashJoinTransform, fnAdapter, pipelineImpl);
    }

    @FunctionalInterface
    interface CreateOutStageFn<T0> {
        <R> GeneralStage<R> get(
                HashJoinTransform<T0, R> hashJoinTransform, FunctionAdapter fnAdapter, PipelineImpl stage);
    }

    private static class TransformAndClause<K, E0, T1, T1_OUT> {
        private final Transform transform;
        private final JoinClause<K, E0, T1, T1_OUT> joinClause;

        @SuppressWarnings("unchecked")
        TransformAndClause(GeneralStage<T1> stage, JoinClause<K, E0, T1, T1_OUT> joinClause) {
            this.transform = transformOf(stage);
            this.joinClause = joinClause;
        }

        Transform transform() {
            return transform;
        }

        JoinClause<K, E0, T1, T1_OUT> clause() {
            return joinClause;
        }
    }
}
