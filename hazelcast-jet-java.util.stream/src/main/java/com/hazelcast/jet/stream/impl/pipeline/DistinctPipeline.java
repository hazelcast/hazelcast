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

import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.processor.DistinctProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class DistinctPipeline<T> extends AbstractIntermediatePipeline<T, T> {

    public DistinctPipeline(StreamContext context, Pipeline<T> upstream) {
        super(context, upstream.isOrdered(), upstream);
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Tuple> toTupleMapper) {
        Distributed.Function<T, Tuple> keyMapper = m -> new JetTuple2<>(m, m);
        Distributed.Function<Tuple, ? extends T> fromTupleMapper = getTupleMapper(upstream, defaultFromTupleMapper());

        if (upstream.isOrdered()) {
            Vertex distinct = vertexBuilder(DistinctProcessor.Factory.class)
                    .addToDAG(dag)
                    .args(fromTupleMapper, keyMapper)
                    .taskCount(1)
                    .build();
            Vertex previous = upstream.buildDAG(dag, distinct, keyMapper);

            if (previous != distinct) {
                edgeBuilder(previous, distinct)
                        .addToDAG(dag)
                        .build();
            }
            return distinct;
        }

        Vertex distinct = vertexBuilder(DistinctProcessor.Factory.class)
                .addToDAG(dag)
                .args(fromTupleMapper, keyMapper)
                .build();

        Vertex previous = upstream.buildDAG(dag, distinct, keyMapper);

        if (previous != distinct) {
            edgeBuilder(previous, distinct)
                    .addToDAG(dag)
                    .processingStrategy(ProcessingStrategy.PARTITIONING)
                    .build();
        }

        Vertex combiner = vertexBuilder(DistinctProcessor.Factory.class)
                .addToDAG(dag)
                .args(defaultFromTupleMapper(), toTupleMapper)
                .build();

        edgeBuilder(distinct, combiner)
                .addToDAG(dag)
                .shuffling(true)
                .processingStrategy(ProcessingStrategy.PARTITIONING)
                .build();

        return combiner;
    }
}
