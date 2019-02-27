/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.GeneralStageWithKey;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;

/**
 * Backing processor for {@link GeneralStageWithKey#rollingAggregate}.
 *
 * @param <T> type of the input item
 * @param <K> type of the key
 * @param <A> type of the accumulator
 * @param <R> type of the output item
 */
public final class RollingAggregateP<T, K, A, R, OUT> extends AbstractProcessor {
    private final FlatMapper<T, OUT> flatMapper;

    private final Map<K, A> keyToAcc = new HashMap<>();
    private final ResettableSingletonTraverser<OUT> outputTraverser = new ResettableSingletonTraverser<>();
    private Traverser<Entry<K, A>> snapshotTraverser;

    public RollingAggregateP(
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp,
            @Nonnull TriFunction<? super T, ? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        this.flatMapper = flatMapper(item -> {
            K key = keyFn.apply(item);
            A acc = keyToAcc.computeIfAbsent(key, k -> aggrOp.createFn().get());
            aggrOp.accumulateFn().accept(acc, item);
            R aggResult = aggrOp.exportFn().apply(acc);
            OUT output = mapToOutputFn.apply(item, key, aggResult);
            if (output != null) {
                outputTraverser.accept(output);
            }
            return outputTraverser;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(keyToAcc.entrySet())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        @SuppressWarnings("unchecked") A old = keyToAcc.put((K) key, (A) value);
        assert old == null : "Duplicate key '" + key + '\'';
    }
}
