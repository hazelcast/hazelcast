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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collector;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;

public class CombineGroupsP<K, V, A, R> extends AbstractProcessor {

    private final Map<K, A> groups = new HashMap<>();
    private final Collector<V, A, R> collector;
    private final Traverser<Entry<K, R>> resultTraverser;

    public CombineGroupsP(Collector<V, A, R> collector) {
        this.collector = collector;
        this.resultTraverser = lazy(() -> traverseStream(groups
                .entrySet().stream()
                .map(item -> entry(item.getKey(), collector.finisher().apply(item.getValue())))
        ));
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Map.Entry<K, A> entry = (Map.Entry) item;
        A value = groups.computeIfAbsent(entry.getKey(), k -> collector.supplier().get());
        collector.combiner().apply(value, entry.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(resultTraverser);
    }

}
