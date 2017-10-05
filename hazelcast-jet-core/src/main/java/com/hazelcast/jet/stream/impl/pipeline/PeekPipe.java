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

import com.hazelcast.core.IList;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;

import java.util.function.Consumer;

import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

class PeekPipe<T> extends AbstractIntermediatePipe<T, T> {

    private final Consumer<? super T> consumer;

    PeekPipe(StreamContext context, Pipe<T> upstream, Consumer<? super T> consumer) {
        super(context, upstream.isOrdered(), upstream);
        checkSerializable(consumer, "consumer");
        this.consumer = consumer;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        String listName = uniqueListName();
        IList<T> list = context.getJetInstance().getList(listName);
        Vertex previous = upstream.buildDAG(dag);
        Vertex writer = dag.newVertex("write-list-" + listName, SinkProcessors.writeListP(listName));
        if (upstream.isOrdered()) {
            writer.localParallelism(1);
        }
        dag.edge(from(previous, 1).to(writer, 0));
        context.addStreamListener(() -> {
            list.forEach(consumer);
            list.destroy();
        });
        return previous;
    }
}
