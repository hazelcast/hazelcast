/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implements the "collector" stage in a hash join transformation. This
 * stage collects the entire joined stream into a hashtable and then
 * broadcasts it to all local second-stage processors.
 */
public class HashJoinCollectP<K, T, V> extends AbstractProcessor {

    private static final BiFunction<Object, Object, Object> MERGE_FN = (o, n) -> {
        if (o instanceof HashJoinArrayList) {
            ((HashJoinArrayList) o).add(n);
            return o;
        } else {
            HashJoinArrayList res = new HashJoinArrayList();
            res.add(o);
            res.add(n);
            return res;
        }
    };

    // the value is either a V or a HashJoinArrayList (if multiple values for
    // the key were observed)
    private final Map<K, Object> lookupTable = new HashMap<>();
    private final Function<T, K> keyFn;
    private final Function<T, V> projectFn;

    private long maxEntries;
    private long numberOfEntries;

    public HashJoinCollectP(@Nonnull Function<T, K> keyFn, @Nonnull Function<T, V> projectFn) {
        this.keyFn = keyFn;
        this.projectFn = projectFn;
    }

    @Override
    protected void init(@Nonnull Processor.Context context) throws Exception {
        maxEntries = context.maxProcessorAccumulatedRecords();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess0(@Nonnull Object item) {
        if (numberOfEntries == maxEntries) {
            throw new AccumulationLimitExceededException();
        }

        T t = (T) item;
        K key = keyFn.apply(t);
        V value = projectFn.apply(t);
        lookupTable.merge(key, value, MERGE_FN);
        numberOfEntries++;
        return true;
    }

    @Override
    public boolean complete() {
        return tryEmit(lookupTable);
    }

    // We need a custom ArrayList subclass because the user's V type could be
    // ArrayList and then the logic that relies on instanceof would break
    static final class HashJoinArrayList extends ArrayList<Object> {

        private static final long serialVersionUID = 1L;

        HashJoinArrayList() {
            super(2);
        }
    }
}
