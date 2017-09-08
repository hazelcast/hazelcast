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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.processor.TransformP;

import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

class TransformPipe<E_IN, E_OUT> extends AbstractIntermediatePipe<E_IN, E_OUT> {

    private final DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer;

    TransformPipe(StreamContext context, Pipe<E_IN> upstream,
                  DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer) {
        super(context, upstream.isOrdered(), upstream);
        this.transformer = transformer;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex previous = upstream.buildDAG(dag);
        // the lambda below must not capture `this`, therefore the instance variable
        // must first be loaded into a local variable
        DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer = this.transformer;
        Vertex transform = dag.newVertex(uniqueVertexName("transform"), () -> new TransformP<>(transformer));
        if (upstream.isOrdered()) {
            transform.localParallelism(1);
        }
        dag.edge(between(previous, transform));
        return transform;
    }

    @Override
    public DistributedStream<E_OUT> filter(DistributedPredicate<? super E_OUT> predicate) {
        // prevent capture of `this`
        DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer = this.transformer;
        return new TransformPipe<>(context, upstream, t -> transformer.apply(t).filter(predicate));
    }

    @Override
    public <R> DistributedStream<R> map(DistributedFunction<? super E_OUT, ? extends R> mapper) {
        // prevent capture of `this`
        DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer = this.transformer;
        return new TransformPipe<>(context, upstream, t -> transformer.apply(t).map(mapper));
    }

    @Override
    public <R> DistributedStream<R> flatMap(DistributedFunction<? super E_OUT, ? extends Stream<? extends R>> mapper) {
        // prevent capture of `this`
        DistributedFunction<Traverser<E_IN>, Traverser<E_OUT>> transformer = this.transformer;
        return new TransformPipe<>(context, upstream,
                t -> transformer.apply(t)
                                .flatMap(item -> traverseStream(mapper.apply(item))));
    }
}
