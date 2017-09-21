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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.singletonList;

/**
 * Batch processor that groups items by key and computes the supplied
 * aggregate operation on each group. The items may originate from one or
 * more inbound edges. The supplied aggregate operation must have as many
 * accumulation functions as there are inbound edges.
 */
public class CoGroupP<K, A, R> extends AbstractProcessor {
    private final List<DistributedFunction<?, ? extends K>> groupKeyFs;
    private final AggregateOperation<A, R> aggrOp;

    private final Map<K, A> keyToAcc = new HashMap<>();
    private final Traverser<Map.Entry<K, R>> resultTraverser;

    public CoGroupP(
            @Nonnull List<DistributedFunction<?, ? extends K>> groupKeyFs,
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        this.groupKeyFs = groupKeyFs;
        this.aggrOp = aggrOp;
        this.resultTraverser = traverseStream(keyToAcc
                .entrySet().stream()
                .map(e -> entry(e.getKey(), this.aggrOp.finishFn().apply(e.getValue()))));
    }

    public <T> CoGroupP(
            @Nonnull DistributedFunction<? super T, ? extends K> groupKeyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        this(singletonList(groupKeyFn), aggrOp);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Function<Object, ? extends K> keyFn = (Function<Object, ? extends K>) groupKeyFs.get(ordinal);
        K key = keyFn.apply(item);
        A acc = keyToAcc.computeIfAbsent(key, k -> aggrOp.createFn().get());
        aggrOp.accumulateFn(ordinal).accept(acc, item);
        return true;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(resultTraverser);
    }
}
