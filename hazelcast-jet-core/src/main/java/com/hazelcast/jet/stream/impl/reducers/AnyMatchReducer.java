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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AnyMatchP;

import java.util.function.Predicate;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.processor.Sinks.writeList;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

public class AnyMatchReducer<T> implements Reducer<T, Boolean> {

    private final Predicate<? super T> predicate;

    public AnyMatchReducer(Predicate<? super T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Boolean reduce(StreamContext context, Pipeline<? extends T> upstream) {
        String listName = uniqueListName();

        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);

        Vertex anyMatch = dag.newVertex("any-match", () -> new AnyMatchP<>(predicate));
        Vertex writer = dag.newVertex("write-" + listName, writeList(listName));

        dag.edge(between(previous, anyMatch))
           .edge(between(anyMatch, writer));

        executeJob(context, dag);

        IList<Boolean> results = context.getJetInstance().getList(listName);
        boolean result = anyMatch(results);
        results.destroy();
        return result;
    }

    private static boolean anyMatch(IList<Boolean> results) {
        for (Boolean result : results) {
            if (result) {
                return true;
            }
        }
        return false;
    }

}
