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
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AnyMatchProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;

import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class Matcher {

    private final StreamContext context;

    public Matcher(StreamContext context) {
        this.context = context;
    }

    public <T> boolean anyMatch(Pipeline<T> upstream, Distributed.Predicate<? super T> predicate) {
        DAG dag = new DAG();
        Vertex anymatch = new Vertex(randomName(), () -> new AnyMatchProcessor<>(predicate));
        dag.addVertex(anymatch);
        Vertex previous = upstream.buildDAG(dag);
        if (previous != anymatch) {
            dag.addEdge(new Edge(previous, anymatch));
        }
        IList<Boolean> results = execute(dag, anymatch);
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

    private IList<Boolean> execute(DAG dag, Vertex vertex) {
        String listName = randomName(LIST_PREFIX);
        Vertex writer = new Vertex(randomName(), Processors.listWriter(listName));
        dag.addVertex(writer).addEdge(new Edge(vertex, writer));
        executeJob(context, dag);
        return context.getJetInstance().getList(listName);
    }


}
