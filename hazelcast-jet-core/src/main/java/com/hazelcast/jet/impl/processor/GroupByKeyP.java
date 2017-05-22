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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;

/**
 * Batch processor that groups items by key and computes the supplied
 * aggregate operation on each group.
 */
public class GroupByKeyP<T, K, A, R> extends AbstractProcessor {
    private final Function<? super T, ? extends K> getKeyF;
    private final AggregateOperation<? super T, A, R> aggrOp;

    private final Map<K, A> groups = new HashMap<>();
    private final Traverser<Map.Entry<K, R>> resultTraverser;

    public GroupByKeyP(
            @Nonnull Function<? super T, ? extends K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        this.getKeyF = getKeyF;
        this.aggrOp = aggregateOperation;
        this.resultTraverser = traverseStream(groups
                .entrySet().stream()
                .map(e -> entry(e.getKey(), aggrOp.finishAccumulationF().apply(e.getValue()))));
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        final A acc = groups.computeIfAbsent(getKeyF.apply((T) item), k -> aggrOp.createAccumulatorF().get());
        aggrOp.accumulateItemF().accept(acc, (T) item);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(resultTraverser);
    }
}
