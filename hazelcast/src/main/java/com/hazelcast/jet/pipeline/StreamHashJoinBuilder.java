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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.impl.pipeline.StreamStageImpl;

/**
 * Offers a step-by-step fluent API to build a hash-join pipeline stage.
 * To obtain it, call {@link StreamStage#hashJoinBuilder()} on the primary
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
public class StreamHashJoinBuilder<T0> extends GeneralHashJoinBuilder<T0> {

    StreamHashJoinBuilder(StreamStage<T0> stage0) {
        super(stage0, StreamStageImpl::new);
    }

    /**
     * Builds a new pipeline stage that performs the hash-join operation. Attaches
     * the stage to all the contributing stages.
     *
     * @param mapToOutputFn the function to map the output item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @return the new hash-join pipeline stage
     */
    public <R> StreamStage<R> build(BiFunctionEx<T0, ItemsByTag, R> mapToOutputFn) {
        return (StreamStage<R>) build0(mapToOutputFn);
    }
}
