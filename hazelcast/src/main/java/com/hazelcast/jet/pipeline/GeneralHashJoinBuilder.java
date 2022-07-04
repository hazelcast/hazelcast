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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.AbstractStage;
import com.hazelcast.jet.impl.pipeline.ComputeStageImplBase;
import com.hazelcast.jet.impl.pipeline.FunctionAdapter;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
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
 *
 * @since Jet 3.0
 */
@SuppressWarnings("rawtypes")
public abstract class GeneralHashJoinBuilder<T0> {
    private final GeneralStage<T0> stage0;
    private final PipelineImpl pipelineImpl;
    private final FunctionAdapter fnAdapter;
    private final CreateOutStageFn<T0> createOutStageFn;
    private final Map<Tag<?>, StageAndClause<?, T0, ?, ?>> clauses = new HashMap<>();

    GeneralHashJoinBuilder(GeneralStage<T0> stage0, CreateOutStageFn<T0> createOutStageFn) {
        this.stage0 = stage0;
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
        clauses.put(tag, new StageAndClause<>(stage, joinClause, false));
        return tag;
    }

    /**
     * Adds another contributing pipeline stage to the hash-join operation.
     *
     * If no matching items for returned {@linkplain Tag tag} is found, no
     * records for given key will be added.
     *
     * @param stage the contributing stage
     * @param joinClause specifies how to join the contributing stage
     * @param <K> the type of the join key
     * @param <T1_IN> the type of the contributing stage's data
     * @param <T1> the type of result after applying the projecting transformation
     *             to the contributing stage's data
     * @return the tag that refers to the contributing stage
     * @since Jet 4.1
     */
    public <K, T1_IN, T1> Tag<T1> addInner(BatchStage<T1_IN> stage, JoinClause<K, T0, T1_IN, T1> joinClause) {
        Tag<T1> tag = tag(clauses.size());
        clauses.put(tag, new StageAndClause<>(stage, joinClause, true));
        return tag;
    }

    @SuppressWarnings("unchecked")
    <R> GeneralStage<R> build0(BiFunctionEx<T0, ItemsByTag, R> mapToOutputFn) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        List<Entry<Tag<?>, StageAndClause<?, T0, ?, ?>>> orderedClauses =
                clauses.entrySet().stream()
                       .sorted(Entry.comparingByKey())
                       .collect(toList());
        List<GeneralStage> upstream = concat(
                Stream.of(stage0),
                orderedClauses.stream().map(e -> e.getValue().stage())
        ).collect(toList());
        Stream<? extends JoinClause<?, T0, ?, ?>> joinClauses = orderedClauses
                .stream()
                .map(e -> e.getValue().clause());
        // JoinClause's second param, T0, is the same for all clauses but only
        // before FunctionAdapter treatment. After that it may be T0 or JetEvent<T0>
        // so we are forced to generalize to just ?.
        // The (JoinClause) and (List<JoinClause>) casts are a workaround for JDK 8 compiler bugs.
        List<JoinClause> adaptedClauses = (List<JoinClause>) joinClauses
                .map(joinClause -> fnAdapter.adaptJoinClause((JoinClause) joinClause))
                .collect(toList());
        BiFunctionEx<?, ? super ItemsByTag, ?> adaptedOutputFn = fnAdapter.adaptHashJoinOutputFn(mapToOutputFn);
        // Here we break type safety and assume T0 as the type parameter even though
        // it may actually be JetEvent<T0>, but that difference is invisible at the
        // level of types used on pipeline stages.
        HashJoinTransform<T0, R> hashJoinTransform = new HashJoinTransform(
                upstream.stream().map(AbstractStage::transformOf).collect(toList()),
                adaptedClauses,
                orderedClauses.stream()
                              .map(Entry::getKey)
                              .collect(toList()),
                adaptedOutputFn,
                orderedClauses
                        .stream()
                        .map(e -> e.getValue().inner)
                        .collect(toList()));
        pipelineImpl.connectGeneralStages(upstream, hashJoinTransform);
        return createOutStageFn.get(hashJoinTransform, fnAdapter, pipelineImpl);
    }

    @FunctionalInterface
    interface CreateOutStageFn<T0> {
        <R> GeneralStage<R> get(
                HashJoinTransform<T0, R> hashJoinTransform, FunctionAdapter fnAdapter, PipelineImpl stage);
    }

    private static class StageAndClause<K, E0, T1, T1_OUT> {
        private final GeneralStage<T1> stage;
        private final JoinClause<K, E0, T1, T1_OUT> joinClause;
        private final boolean inner;

        StageAndClause(GeneralStage<T1> stage, JoinClause<K, E0, T1, T1_OUT> joinClause, boolean inner) {
            this.stage = stage;
            this.joinClause = joinClause;
            this.inner = inner;
        }

        GeneralStage<T1> stage() {
            return stage;
        }

        JoinClause<K, E0, T1, T1_OUT> clause() {
            return joinClause;
        }
    }
}
