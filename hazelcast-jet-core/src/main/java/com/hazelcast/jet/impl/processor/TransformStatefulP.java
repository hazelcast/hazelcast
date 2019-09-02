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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static java.lang.Math.min;

public class TransformStatefulP<T, K, S, R, OUT> extends AbstractProcessor {
    static final int MAX_ITEMS_TO_EVICT = 100;
    private static final int HASH_MAP_INITIAL_CAPACITY = 16;
    private static final float HASH_MAP_LOAD_FACTOR = 0.75f;

    @Probe(name = "lateEventsDropped")
    private final AtomicLong lateEventsDropped = new AtomicLong();

    private final long ttl;
    private final Function<Object, ? extends K> keyFn;
    private final ToLongFunction<? super T> timestampFn;
    private final Supplier<? extends S> createFn;
    private final BiFunction<? super S, ? super T, ? extends Traverser<R>> statefulFlatMapFn;
    private final TriFunction<? super T, ? super K, ? super R, ? extends OUT> mapToOutputFn;
    private final FlatMapper<T, OUT> flatMapper = flatMapper(this::flatMapEvent);

    private final Map<K, TimestampedItem<S>> keyToState = new LruHashMap();
    private long currentWm = Long.MIN_VALUE;
    private Traverser<? extends Entry<?, ?>> snapshotTraverser;

    @SuppressWarnings("unchecked")
    public TransformStatefulP(
            long ttl,
            @Nonnull Function<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull BiFunction<? super S, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nonnull TriFunction<? super T, ? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        this.ttl = ttl > 0 ? ttl : Long.MAX_VALUE;
        this.keyFn = (Function<Object, ? extends K>) keyFn;
        this.timestampFn = timestampFn;
        this.createFn = createFn;
        this.statefulFlatMapFn = statefulFlatMapFn;
        this.mapToOutputFn = mapToOutputFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
    }

    @Nonnull
    private Traverser<OUT> flatMapEvent(T event) {
        long timestamp = timestampFn.applyAsLong(event);
        if (timestamp < currentWm) {
            logLateEvent(getLogger(), currentWm, event);
            lazyIncrement(lateEventsDropped);
            return Traversers.empty();
        }
        K key = keyFn.apply(event);
        S state = resolveState(key, timestamp);
        return applyOutputFnOptimized(event, key, statefulFlatMapFn.apply(state, event));
    }

    @Nonnull
    private S resolveState(K key, long timestamp) {
        TimestampedItem<S> tsAndState = keyToState.get(key);
        if (tsAndState != null) {
            long lastTouched = tsAndState.timestamp();
            if (lastTouched < Util.subtractClamped(currentWm, ttl)) {
                tsAndState = null;
            } else if (timestamp > lastTouched) {
                tsAndState.setTimestamp(timestamp);
            }
        }
        if (tsAndState == null) {
            tsAndState = new TimestampedItem<>(timestamp, createFn.get());
            keyToState.put(key, tsAndState);
        }
        return tsAndState.item();
    }

    @Nonnull
    private Traverser<OUT> applyOutputFnOptimized(T event, K key, Traverser<R> resultTrav) {
        if (!(resultTrav instanceof ResettableSingletonTraverser)) {
            return resultTrav.map(r -> mapToOutputFn.apply(event, key, r));
        }
        R r = resultTrav.next();
        if (r != null) {
            ResettableSingletonTraverser<OUT> rst = (ResettableSingletonTraverser<OUT>) resultTrav;
            rst.accept(mapToOutputFn.apply(event, key, r));
            return rst;
        } else {
            return Traversers.empty();
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        currentWm = watermark.timestamp();
        long countEvicted = 0;
        for (Iterator<Entry<K, TimestampedItem<S>>> iter = keyToState.entrySet().iterator(); iter.hasNext();) {
            long timestamp = iter.next().getValue().timestamp();
            if (timestamp >= currentWm - ttl) {
                break;
            }
            iter.remove();
            if (++countEvicted == MAX_ITEMS_TO_EVICT) {
                break;
            }
        }
        return super.tryProcessWatermark(watermark);
    }

    private enum SnapshotKeys {
        WATERMARK
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.<Entry<?, ?>>traverseIterable(keyToState.entrySet())
                    .append(entry(broadcastKey(SnapshotKeys.WATERMARK), currentWm))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            assert ((BroadcastKey) key).key() == SnapshotKeys.WATERMARK : "Unexpected " + key;
            long wm = (long) value;
            currentWm = (currentWm == Long.MIN_VALUE) ? wm : min(currentWm, wm);
        } else {
            @SuppressWarnings("unchecked")
            TimestampedItem<S> old = keyToState.put((K) key, (TimestampedItem<S>) value);
            assert old == null : "Duplicate key '" + key + '\'';
        }
    }

    private class LruHashMap extends LinkedHashMap<K, TimestampedItem<S>> {
        LruHashMap() {
            super(HASH_MAP_INITIAL_CAPACITY, HASH_MAP_LOAD_FACTOR, true);
        }

        @Override
        protected boolean removeEldestEntry(@Nonnull Entry<K, TimestampedItem<S>> eldest) {
            return eldest.getValue().timestamp() < Util.subtractClamped(currentWm, ttl);
        }
    }
}
