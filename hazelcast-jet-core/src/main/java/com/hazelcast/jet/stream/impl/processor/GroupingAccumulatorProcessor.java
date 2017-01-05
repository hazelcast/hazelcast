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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.hazelcast.jet.Suppliers.lazyIterate;
import static com.hazelcast.jet.Suppliers.map;

public class GroupingAccumulatorProcessor<T, K, V, A, R> extends AbstractProcessor {

    private Map<K, A> cache = new HashMap<>();
    private Function<? super T, ? extends K> classifier;
    private Collector<V, A, R> collector;
    private Supplier<Entry<K, A>> cacheEntrySupplier;

    public GroupingAccumulatorProcessor(Function<? super T, ? extends K> classifier, Collector<V, A, R> collector) {
        this.classifier = classifier;
        this.collector = collector;
        this.cacheEntrySupplier = map(
                lazyIterate(() -> cache.entrySet().iterator()),
                entry -> new SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        Map.Entry<K, V> entry = new SimpleImmutableEntry<>(classifier.apply((T) item), (V) item);
        A value = cache.get(entry.getKey());
        if (value == null) {
            value = collector.supplier().get();
            cache.put(entry.getKey(), value);
        }
        collector.accumulator().accept(value, entry.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        final boolean done = emitCooperatively(cacheEntrySupplier);
        if (done) {
            cache = null;
            classifier = null;
            collector = null;
            cacheEntrySupplier = null;
        }
        return done;
    }
}
