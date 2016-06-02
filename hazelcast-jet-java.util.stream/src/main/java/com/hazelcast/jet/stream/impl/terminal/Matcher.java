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

package com.hazelcast.jet.stream.impl.terminal;

import com.hazelcast.core.IList;
import com.hazelcast.jet.api.dag.DAGImpl;
import com.hazelcast.jet.api.data.tuple.JetTuple2;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AnyMatchProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeApplication;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class Matcher {

    private final StreamContext context;

    public Matcher(StreamContext context) {
        this.context = context;
    }

    public <T> boolean anyMatch(Pipeline<T> upstream, Distributed.Predicate<? super T> predicate) {
        DAGImpl dag = new DAGImpl();
        Distributed.Function<Tuple, ? extends T> fromTupleMapper = getTupleMapper(upstream, defaultFromTupleMapper());
        Vertex vertex = vertexBuilder(AnyMatchProcessor.Factory.class)
                .addToDAG(dag)
                .args(fromTupleMapper, toTupleMapper(), predicate)
                .build();

        Vertex previous = upstream.buildDAG(dag, vertex, toTupleMapper());
        if (previous != vertex) {
            edgeBuilder(previous, vertex)
                    .addToDAG(dag)
                    .build();
        }

        IList<Boolean> results = execute(dag, vertex);
        boolean result = anyMatch(results);
        results.destroy();
        return result;
    }

    private boolean anyMatch(IList<Boolean> results) {
        for (Boolean result : results) {
            if (result) {
                return true;
            }
        }
        return false;
    }

    private IList<Boolean> execute(DAGImpl dag, Vertex vertex) {
        IList<Boolean> list = context.getHazelcastInstance().getList(randomName(LIST_PREFIX));
        vertex.addSinkList(list.getName());
        executeApplication(context, dag);
        return list;
    }

    private <T, U extends T> Distributed.Function<U, Tuple> toTupleMapper() {
        return  o -> new JetTuple2<Object, T>(0, o);
    }


}
