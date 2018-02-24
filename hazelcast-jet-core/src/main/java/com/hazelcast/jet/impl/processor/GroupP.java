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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singletonList;

/**
 * Batch processor that groups items by key and computes the supplied
 * aggregate operation on each group. The items may originate from one or
 * more inbound edges. The supplied aggregate operation must have as many
 * accumulation functions as there are inbound edges.
 */
public class GroupP<K, A, R, OUT> extends AbstractProcessor {
    @Nonnull private final List<DistributedFunction<?, ? extends K>> groupKeyFns;
    @Nonnull private final AggregateOperation<A, R> aggrOp;

    private final Map<K, A> keyToAcc = new HashMap<>();
    private final Traverser<OUT> resultTraverser;

    public GroupP(
            @Nonnull List<DistributedFunction<?, ? extends K>> groupKeyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull BiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        checkTrue(groupKeyFns.size() == aggrOp.arity(), groupKeyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        this.groupKeyFns = groupKeyFns;
        this.aggrOp = aggrOp;
        this.resultTraverser = traverseStream(keyToAcc
                .entrySet().stream()
                .map(e -> mapToOutputFn.apply(e.getKey(), aggrOp.finishFn().apply(e.getValue()))));
    }

    public <T> GroupP(
            @Nonnull DistributedFunction<? super T, ? extends K> groupKeyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull BiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        this(singletonList(groupKeyFn), aggrOp, mapToOutputFn);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Function<Object, ? extends K> keyFn = (Function<Object, ? extends K>) groupKeyFns.get(ordinal);
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
