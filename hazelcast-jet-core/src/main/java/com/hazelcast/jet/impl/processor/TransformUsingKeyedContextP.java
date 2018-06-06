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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Traversers.traverseIterable;

/**
 * Backing processor for {@link
 * com.hazelcast.jet.pipeline.GeneralStageWithGrouping#mapUsingContext}.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <K> key type
 * @param <R> emitted item type
 */
public final class TransformUsingKeyedContextP<C, T, K, R> extends AbstractProcessor {
    private final ContextFactory<C> contextFactory;
    private final DistributedFunction<? super T, ? extends K> keyFn;
    private final DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
            ? extends Traverser<? extends R>> flatMapFn;

    private JetInstance jet;
    private final Map<K, C> contextObjects = new HashMap<>();
    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();
    private Traverser<Entry<K, C>> snapshotTraverser;

    /**
     * Constructs a processor with the given mapping function.
     */
    public TransformUsingKeyedContextP(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    ? extends Traverser<? extends R>> flatMapFn
    ) {
        this.contextFactory = contextFactory;
        this.keyFn = keyFn;
        this.flatMapFn = flatMapFn;
    }

    @Override
    protected void init(@Nonnull Context context) {
        jet = context.jetInstance();
    }

    @Override
    public boolean isCooperative() {
        return contextFactory.isCooperative();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (outputTraverser == null) {
            K key = keyFn.apply((T) item);
            C contextObject = contextObjects.computeIfAbsent(key, k -> contextFactory.createFn().apply(jet));
            outputTraverser = flatMapFn.apply(singletonTraverser, contextObject, (T) item);
        }
        if (emitFromTraverser(outputTraverser)) {
            outputTraverser = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(contextObjects.entrySet())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        C old = contextObjects.put((K) key, (C) value);
        assert old == null : "Duplicate key '" + key + "'";
    }

    @Override
    public void close(@Nullable Throwable error) {
        for (C ctx : contextObjects.values()) {
            contextFactory.destroyFn().accept(ctx);
        }
        contextObjects.clear();
    }
}
