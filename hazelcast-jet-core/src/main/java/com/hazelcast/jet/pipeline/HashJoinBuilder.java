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
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.pipeline.BatchStageImpl;

/**
 * Offers a step-by-step fluent API to build a hash-join pipeline stage.
 * To obtain it, call {@link BatchStage#hashJoinBuilder()} on the primary
 * stage, the one whose data will be enriched from all other stages.
 * <p>
 * Collect all the tags returned from {@code add()} and use them to retrieve
 * the enriching items from {@link ItemsByTag} you get in the result. Retrieve
 * the tag of the first stage (from which you obtained the builder) by calling
 * {@link #tag0()}.
 * <p>
 * This object is mainly intended to build a hash-join of the primary stage
 * with three or more contributing stages. For one or two stages, prefer the
 * direct {@code stage.hashJoin(...)} calls because they offer more static
 * type safety.
 *
 * @param <T0> the type of the items in the primary stage
 */
public class HashJoinBuilder<T0> extends GeneralHashJoinBuilder<T0> {

    HashJoinBuilder(BatchStage<T0> stage0) {
        super(stage0, BatchStageImpl::new);
    }

    /**
     * Builds a new pipeline stage that performs the hash-join operation. Attaches
     * the stage to all the contributing stages.
     *
     * @return the new hash-join pipeline stage
     */
    public <R> BatchStage<R> build(DistributedBiFunction<T0, ItemsByTag, R> mapToOutputFn) {
        return (BatchStage<R>) build0(mapToOutputFn);
    }
}
