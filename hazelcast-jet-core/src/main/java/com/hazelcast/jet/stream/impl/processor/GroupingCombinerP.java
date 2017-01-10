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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Traverser;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collector;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;

public class GroupingCombinerP<K, V, A, R> extends AbstractProcessor {

    private Map<K, A> groups = new HashMap<>();
    private Collector<V, A, R> collector;
    private Traverser<Entry<K, R>> resultTraverser;

    public GroupingCombinerP(Collector<V, A, R> collector) {
        this.collector = collector;
        this.resultTraverser = lazy(() -> traverseStream(groups
                .entrySet().stream()
                .map(item -> new SimpleImmutableEntry<>(item.getKey(), collector.finisher().apply(item.getValue())))
        ));
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        Map.Entry<K, A> entry = (Map.Entry) item;
        A value = groups.computeIfAbsent(entry.getKey(), k -> collector.supplier().get());
        collector.combiner().apply(value, entry.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        final boolean done = emitCooperatively(resultTraverser);
        if (done) {
            groups = null;
            collector = null;
            resultTraverser = null;
        }
        return done;
    }

}
