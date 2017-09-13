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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.pipeline.datamodel.Tag.tag;
import static java.util.stream.Collectors.toList;

/**
 * Offers a step-by-step fluent API to build a co-grouping pipeline stage by
 * adding any number of contributing stages. To obtain the builder, call
 * {@link ComputeStage#coGroupBuilder(DistributedFunction)
 * stage.coGroupBuilder()} on the stage that will become the zero-indexed
 * contributor to the co-grouping operation.
 * <p>
 * This object is primarily intended to build a co-grouping of four or more
 * stages; for up to three stages the direct {@code stage.coGroup(...)}
 * calls should be preferred because they offer more static type safety.
 *
 * @param <K> the type of the grouping key
 * @param <E0> the type of item in stream-0
 */
public class CoGroupBuilder<K, E0> {
    private final List<CoGroupClause<?, K>> clauses = new ArrayList<>();

    CoGroupBuilder(ComputeStage<E0> s, DistributedFunction<? super E0, K> groupKeyFn) {
        add(s, groupKeyFn);
    }

    /**
     * Returns the tag referring to the 0-indexed contributing pipeline
     * stage, the one from which this builder was obtained.
     */
    public Tag<E0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another contributing pipeline stage to the co-grouping operation.
     *
     * @param stage the pipeline stage to be co-grouped
     * @param groupKeyFn a function that will extract the key from the data items of the
     *                  pipeline stage
     * @param <E> type of items on the pipeline stage
     * @return the tag referring to the pipeline stage
     */
    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(ComputeStage<E> stage, DistributedFunction<? super E, K> groupKeyFn) {
        clauses.add(new CoGroupClause<>(stage, groupKeyFn));
        return (Tag<E>) tag(clauses.size() - 1);
    }

    /**
     * Builds a new pipeline stage that performs the co-grouping operation. The
     * stage is attached to all the contributing stages.
     *
     * @param aggrOp the aggregate operation to perform on the co-grouped items
     * @param <A> the type of the accumulator in the aggregate operation
     * @param <R> the type of the result of aggregation
     * @return the co-grouping pipeline stage
     */
    @SuppressWarnings("unchecked")
    public <A, R> ComputeStage<Tuple2<K, R>> build(AggregateOperation<A, R> aggrOp) {
        List<ComputeStage> upstream = clauses
                .stream()
                .map(CoGroupClause::stage)
                .collect(toList());
        CoGroupTransform<K, A, R> transform = new CoGroupTransform<>(clauses
                .stream()
                .map(CoGroupClause::groupKeyFn)
                .collect(toList()), aggrOp);
        PipelineImpl pipeline = (PipelineImpl) clauses.get(0).stage.getPipeline();
        return pipeline.attach(upstream, transform);
    }

    private static class CoGroupClause<E, K> {
        private final ComputeStage<E> stage;
        private final DistributedFunction<? super E, K> groupKeyFn;

        CoGroupClause(ComputeStage<E> stage, DistributedFunction<? super E, K> groupKeyFn) {
            this.stage = stage;
            this.groupKeyFn = groupKeyFn;
        }

        ComputeStage<E> stage() {
            return stage;
        }

        DistributedFunction<? super E, K> groupKeyFn() {
            return groupKeyFn;
        }
    }
}
