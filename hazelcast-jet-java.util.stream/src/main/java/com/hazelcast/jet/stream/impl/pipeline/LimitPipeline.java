/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.processor.LimitProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class LimitPipeline<T> extends AbstractIntermediatePipeline<T, T> {
    private final long limit;

    public LimitPipeline(StreamContext context, Pipeline<T> upstream, long limit) {
        super(context, upstream.isOrdered(), upstream);
        this.limit = limit;
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Tuple> toTupleMapper) {
        Vertex first = vertexBuilder(LimitProcessor.class)
                .addToDAG(dag)
                .args(getTupleMapper(upstream, defaultFromTupleMapper()), toTupleMapper(), limit)
                .taskCount(1)
                .build();

        Vertex previous = upstream.buildDAG(dag, first, toTupleMapper());

        if (first != previous) {
            edgeBuilder(previous, first)
                    .addToDAG(dag)
                    .build();
        }

        if (upstream.isOrdered()) {
            return first;
        }

        Vertex second = vertexBuilder(LimitProcessor.class)
                .addToDAG(dag)
                .args(defaultFromTupleMapper(), toTupleMapper, limit)
                .taskCount(1)
                .build();

        edgeBuilder(first, second)
                .addToDAG(dag)
                .shuffling(true)
                .shufflingStrategy(new IListBasedShufflingStrategy(randomName()))
                .build();

        return second;
    }
}
