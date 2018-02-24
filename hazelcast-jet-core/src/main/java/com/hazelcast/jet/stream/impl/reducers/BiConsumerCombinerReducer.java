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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.pipeline.Pipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.stream.impl.reducers.CollectorReducer.buildAccumulator;
import static com.hazelcast.jet.stream.impl.reducers.CollectorReducer.buildCombiner;
import static com.hazelcast.jet.stream.impl.reducers.CollectorReducer.execute;

/**
 * A variation of {@link CollectorReducer} which has the combiner
 * as a {@code BiConsumer} instead.
 */
public class BiConsumerCombinerReducer<T, R> implements Reducer<T, R> {

    private final Supplier<R> supplier;
    private final BiConsumer<R, ? super T> accumulator;
    private final BiConsumer<R, R> combiner;

    public BiConsumerCombinerReducer(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator,
                                     BiConsumer<R, R> combiner) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
    }

    @Override
    public R reduce(StreamContext context, Pipe<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, supplier, accumulator);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, combiner);

        return execute(context, dag, combinerVertex, DistributedFunction.identity());
    }
}
