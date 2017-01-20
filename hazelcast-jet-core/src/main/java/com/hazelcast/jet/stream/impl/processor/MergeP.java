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

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;

public class MergeP<T, K, V> extends AbstractProcessor {

    private final Function<? super T, ? extends K> keyMapper;
    private final Function<? super T, ? extends V> valueMapper;
    private final BinaryOperator<V> merger;
    private final Map<K, V> merged = new HashMap<>();
    private final Traverser<Entry<K, V>> resultTraverser;

    public MergeP(Function<? super T, ? extends K> keyMapper,
                  Function<? super T, ? extends V> valueMapper,
                  BinaryOperator<V> merger
    ) {
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.merger = merger;
        this.resultTraverser = lazy(() -> traverseStream(merged
                .entrySet().stream()
                .map(item -> new SimpleImmutableEntry<>(item.getKey(), item.getValue()))
        ));
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Map.Entry<K, V> entry;
        if (keyMapper == null || valueMapper == null) {
            entry = (Map.Entry<K, V>) item;
        } else {
            entry = new SimpleImmutableEntry<>(keyMapper.apply((T) item), valueMapper.apply((T) item));
        }
        V value = merged.get(entry.getKey());
        if (value == null) {
            merged.put(entry.getKey(), entry.getValue());
        } else {
            merged.put(entry.getKey(), merger.apply(value, entry.getValue()));
        }
        return true;
    }

    @Override
    public boolean complete() {
        return emitCooperatively(resultTraverser);
    }

}
