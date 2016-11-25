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
import java.util.Iterator;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class MergeProcessor<T, K, V> extends AbstractProcessor {

    private final Function<? super T, ? extends K> keyMapper;
    private final Function<? super T, ? extends V> valueMapper;
    private final BinaryOperator<V> merger;
    private final Map<K, V> cache = new HashMap<>();
    private Iterator<Map.Entry<K, V>> iterator;

    public MergeProcessor(Function<? super T, ? extends K> keyMapper,
                          Function<? super T, ? extends V> valueMapper, BinaryOperator<V> merger) {
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.merger = merger;
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        Map.Entry<K, V> entry;
        if (keyMapper == null || valueMapper == null) {
            entry = (Map.Entry<K, V>) item;
        } else {
            entry = new SimpleImmutableEntry<>(keyMapper.apply((T) item), valueMapper.apply((T) item));
        }
        V value = cache.get(entry.getKey());
        if (value == null) {
            cache.put(entry.getKey(), entry.getValue());
        } else {
            cache.put(entry.getKey(), merger.apply(value, entry.getValue()));
        }
        return true;
    }

    @Override
    public boolean complete() {
        if (iterator == null) {
            iterator = cache.entrySet().iterator();
        }
        while (iterator.hasNext() && !getOutbox().isHighWater()) {
            Map.Entry<K, V> next = iterator.next();
            emit(new SimpleImmutableEntry<>(next.getKey(), next.getValue()));
        }
        return !iterator.hasNext();
    }

}
