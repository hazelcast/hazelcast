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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Implements the "collector" stage in a hash join transformation. This
 * stage collects the entire joined stream into a hashmap and then
 * broadcasts it to all local second-stage processors.
 */
public class HashJoinCollectP<K, E, V> extends AbstractProcessor {
    private final Map<K, V> map = new HashMap<>();
    @Nonnull private final Function<E, K> keyFn;
    @Nonnull private final Function<E, V> projectFn;

    public HashJoinCollectP(@Nonnull Function<E, K> keyFn, @Nonnull Function<E, V> projectFn) {
        this.keyFn = keyFn;
        this.projectFn = projectFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        E e = (E) item;
        K key = keyFn.apply(e);
        V value = projectFn.apply(e);
        V previous = map.put(key, value);
        if (previous != null) {
            throw new IllegalStateException("Duplicate values for key " + key + ": " + previous + " and " + value);
        }
        return true;
    }

    @Override
    public boolean complete() {
        return tryEmit(map);
    }
}
