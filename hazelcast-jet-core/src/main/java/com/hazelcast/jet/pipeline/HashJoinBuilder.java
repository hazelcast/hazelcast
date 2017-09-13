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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.pipeline.datamodel.ItemsByTag;
import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;
import com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.pipeline.datamodel.Tag.tag;
import static com.hazelcast.jet.pipeline.datamodel.Tag.tag0;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Offers a step-by-step fluent API to build a hash-join pipeline stage.
 * To obtain it, call {@link ComputeStage#hashJoinBuilder()} on the primary
 * stage, whose data will be enriched from all other stages.
 * <p>
 * This object is mainly intended to build a hash-join of the primary stage
 * with three or more contributing stages. For one or two stages the direct
 * {@code stage.hashJoin(...)} calls should be preferred because they offer
 * more static type safety.
 *
 * @param <E0> the type of the stream-0 item
 */
public class HashJoinBuilder<E0> {
    private final Map<Tag<?>, StageAndClause> clauses = new HashMap<>();

    HashJoinBuilder(ComputeStage<E0> stage0) {
        add(stage0, null);
    }

    /**
     * Adds another contributing pipeline stage to the hash-join operation.
     *
     * @param stage the contributing stage
     * @param joinClause specifies how to join the contributing stage
     * @param <K> the type of the join key
     * @param <E1_IN> the type of the contributing stage's data
     * @param <E1> the type of result after applying the projecting transformation
     *             to the contributing stage's data
     * @return the tag that refers to the contributing stage
     */
    public <K, E1_IN, E1> Tag<E1> add(ComputeStage<E1_IN> stage, JoinClause<K, E0, E1_IN, E1> joinClause) {
        Tag<E1> tag = tag(clauses.size());
        clauses.put(tag, new StageAndClause<>(stage, joinClause));
        return tag;
    }

    /**
     * Builds a new pipeline stage that performs the hash-join operation. The
     * stage is attached to all the contributing stages.
     *
     * @return the hash-join pipeline stage
     */
    @SuppressWarnings("unchecked")
    public ComputeStage<Tuple2<E0, ItemsByTag>> build() {
        List<Entry<Tag<?>, StageAndClause>> orderedClauses = clauses.entrySet().stream()
                                                                    .sorted(comparing(Entry::getKey))
                                                                    .collect(toList());
        List<ComputeStage> upstream = orderedClauses.stream()
                                                    .map(e -> e.getValue().stage())
                                                    .collect(toList());
        // A probable javac bug forced us to extract this variable
        Stream<JoinClause<?, E0, ?, ?>> joinClauses = orderedClauses
                .stream()
                .skip(1)
                .map(e -> e.getValue().clause());
        HashJoinTransform<E0> hashJoinTransform = new HashJoinTransform<>(
                joinClauses.collect(toList()),
                orderedClauses.stream()
                              .skip(1)
                              .map(Entry::getKey)
                              .collect(toList()));
        PipelineImpl pipeline = (PipelineImpl) clauses.get(tag0()).stage().getPipeline();
        return pipeline.attach(upstream, hashJoinTransform);
    }

    private static class StageAndClause<K, E0, E1, E1_OUT> {
        private final ComputeStage<E1> stage;
        private final JoinClause<K, E0, E1, E1_OUT> joinClause;

        StageAndClause(ComputeStage<E1> stage, JoinClause<K, E0, E1, E1_OUT> joinClause) {
            this.stage = stage;
            this.joinClause = joinClause;
        }

        ComputeStage<E1> stage() {
            return stage;
        }

        JoinClause<K, E0, E1, E1_OUT> clause() {
            return joinClause;
        }
    }
}
