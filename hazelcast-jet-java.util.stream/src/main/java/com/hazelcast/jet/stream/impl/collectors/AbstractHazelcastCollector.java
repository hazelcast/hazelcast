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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.dag.DAGImpl;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.tap.SinkTap;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import static com.hazelcast.jet.stream.impl.StreamUtil.executeApplication;

public abstract class AbstractHazelcastCollector<T, R> extends AbstractCollector<T, Object, R> {

    @Override
    public R collect(StreamContext context, Pipeline<? extends T> upstream) {
        R target = getTarget(context.getHazelcastInstance());
        DAGImpl dag = new DAGImpl();
        Vertex vertex = upstream.buildDAG(dag, null, toTupleMapper());
        vertex.addSinkTap(getSinkTap());
        executeApplication(context, dag);
        return target;
    }

    protected abstract R getTarget(HazelcastInstance instance);

    protected abstract <U extends T> Distributed.Function<U, Tuple> toTupleMapper();

    protected abstract SinkTap getSinkTap();
}
