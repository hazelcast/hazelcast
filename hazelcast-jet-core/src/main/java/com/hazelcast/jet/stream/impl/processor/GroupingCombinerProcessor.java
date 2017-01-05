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

package com.hazelcast.jet.stream.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collector;

public class GroupingCombinerProcessor<T, K, V, A, R> extends AbstractProcessor {

    private final Map<K, A> cache = new HashMap<>();
    private final Collector<V, A, R> collector;
    private Iterator<Map.Entry<K, A>> iterator;

    public GroupingCombinerProcessor(Collector<V, A, R> collector) {
        this.collector = collector;
    }


    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        Map.Entry<K, A> entry = (Map.Entry) item;
        A value = cache.get(entry.getKey());
        if (value == null) {
            value = collector.supplier().get();
            cache.put(entry.getKey(), value);
        }
        collector.combiner().apply(value, entry.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        if (iterator == null) {
            iterator = cache.entrySet().iterator();
        }
        while (iterator.hasNext() && !getOutbox().isHighWater()) {
            Map.Entry<K, A> next = iterator.next();
            K key = next.getKey();
            R value = collector.finisher().apply(next.getValue());
            emit(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }
        return !iterator.hasNext();
    }

}
