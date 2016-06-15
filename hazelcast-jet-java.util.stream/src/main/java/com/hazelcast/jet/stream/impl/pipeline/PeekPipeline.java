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

import com.hazelcast.core.IList;
import com.hazelcast.jet.dag.tap.ListSink;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.processor.PassthroughProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.DEFAULT_TASK_COUNT;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class PeekPipeline<T> extends AbstractIntermediatePipeline<T, T> {

    private final Distributed.Consumer<? super T> consumer;

    public PeekPipeline(StreamContext context, Pipeline<T> upstream,
                        Distributed.Consumer<? super T> consumer) {
        super(context, upstream.isOrdered(), upstream);
        this.consumer = consumer;
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Tuple> toTupleMapper) {
        String listName = randomName();
        IList<T> list = context.getHazelcastInstance().getList(listName);
        Distributed.Function<T, Tuple> toTuple = v -> new JetTuple2<>(0, v);
        Vertex previous = upstream.buildDAG(dag, null, toTuple);
        previous.addSink(new ListSink(list));

        int taskCount = upstream.isOrdered() ? 1 : DEFAULT_TASK_COUNT;

        //This vertex is necessary to convert the input to format suitable for list
        Vertex vertex = vertexBuilder(PassthroughProcessor.Factory.class)
                .addToDAG(dag)
                .args(defaultFromTupleMapper(), toTupleMapper)
                .taskCount(taskCount)
                .build();

        edgeBuilder(previous, vertex)
                .addToDAG(dag)
                .build();

        context.addStreamListener(() -> {
            list.forEach(consumer::accept);
            list.destroy();
        });
        return vertex;
    }
}
