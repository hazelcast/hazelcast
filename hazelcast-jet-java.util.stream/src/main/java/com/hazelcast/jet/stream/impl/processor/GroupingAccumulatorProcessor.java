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

import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet2.impl.AbstractProcessor;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

public class GroupingAccumulatorProcessor<T, K, V, A, R> extends AbstractProcessor {

    private final Map<K, A> cache = new HashMap<>();
    private final Function<? super T, ? extends K> classifier;
    private final Collector<V, A, R> collector;
    private Iterator<Map.Entry<K, A>> finalizationIterator;

    public GroupingAccumulatorProcessor(Function<? super T, ? extends K> classifier, Collector<V, A, R> collector) {
        this.classifier = classifier;
        this.collector = collector;
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        Map.Entry<K, V> entry = toEntryMapper().apply((T) item);
        A value = this.cache.get(entry.getKey());
        if (value == null) {
            value = collector.supplier().get();
            this.cache.put(entry.getKey(), value);
        }
        collector.accumulator().accept(value, entry.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        if (finalizationIterator == null) {
            finalizationIterator = cache.entrySet().iterator();
        }
        while (finalizationIterator.hasNext() && !getOutbox().isHighWater()) {
            Map.Entry<K, A> next = finalizationIterator.next();
            emit(new AbstractMap.SimpleImmutableEntry<>(next.getKey(), next.getValue()));
        }
        return !finalizationIterator.hasNext();
    }

    private <U extends T> Distributed.Function<U, Map.Entry> toEntryMapper() {
        return v -> new AbstractMap.SimpleImmutableEntry<>(classifier.apply(v), v);
    }

}
