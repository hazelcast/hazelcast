/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.FlatMapTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalFlatMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.GlobalMapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapStatefulTransform;
import com.hazelcast.jet.impl.pipeline.transform.MapTransform;
import com.hazelcast.jet.impl.pipeline.transform.SortTransform;
import com.hazelcast.jet.impl.pipeline.transform.TimestampTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;


public final class TransformFactory {

    private TransformFactory() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> AbstractTransform timestampTransform(
            Transform upstream,
            long allowedLag,
            ToLongFunctionEx<T> timestampFn
    ) {
        return new TimestampTransform(upstream, eventTimePolicy(
                timestampFn,
                (item, ts) -> jetEvent(ts, item),
                limitingLag(allowedLag),
                0, 0, DEFAULT_IDLE_TIMEOUT
        ));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> AbstractTransform sortTransform(Transform upstream, ComparatorEx<T> comparator) {
        return new SortTransform(upstream, comparator);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T, R> AbstractTransform mapTransform(Transform upstream, FunctionAdapter fnAdapter, FunctionEx<T, R> mapFn) {
        return new MapTransform("map", upstream, fnAdapter.adaptMapFn(mapFn));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> AbstractTransform filterTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            PredicateEx<T> filterFn
    ) {
        PredicateEx adaptedFn = fnAdapter.adaptFilterFn(filterFn);
        return new MapTransform("filter", upstream, t -> adaptedFn.test(t) ? t : null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T, R> AbstractTransform flatMapTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            FunctionEx<? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return new FlatMapTransform("flat-map", upstream, fnAdapter.adaptFlatMapFn(flatMapFn));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <S, T, R> AbstractTransform globalMapStatefulTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            SupplierEx<? extends S> createFn,
            BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return new GlobalMapStatefulTransform(
                upstream,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulMapFn((s, k, t) -> mapFn.apply(s, t))
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <S, T, R> AbstractTransform globalFlatMapStatefulTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            SupplierEx<? extends S> createFn,
            BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return new GlobalFlatMapStatefulTransform(
                upstream,
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.<S, Object, T, R>adaptStatefulFlatMapFn((s, k, t) -> flatMapFn.apply(s, t))
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, S, T, R> AbstractTransform mapStatefulTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            long ttl,
            FunctionEx<? super T, ? extends K> keyFn,
            SupplierEx<? extends S> createFn,
            TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return new MapStatefulTransform(
                upstream,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulMapFn(mapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFn(onEvictFn) : null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <K, S, T, R> AbstractTransform flatMapStatefulTransform(
            Transform upstream,
            FunctionAdapter fnAdapter,
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return new FlatMapStatefulTransform(
                upstream,
                ttl,
                fnAdapter.adaptKeyFn(keyFn),
                fnAdapter.adaptTimestampFn(),
                createFn,
                fnAdapter.adaptStatefulFlatMapFn(flatMapFn),
                onEvictFn != null ? fnAdapter.adaptOnEvictFlatMapFn(onEvictFn) : null);
    }
}
