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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.function.WindowResultFunction;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.JoinClause;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.impl.pipeline.JetEvent.jetEvent;
import static com.hazelcast.jet.pipeline.JoinClause.onKeys;


public class FunctionAdapter {

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedFunction<?, ?> adaptKeyFn(@Nonnull DistributedFunction<?, ?> keyFn) {
        return keyFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedFunction<?, ?> adaptMapFn(@Nonnull DistributedFunction mapFn) {
        return mapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedBiFunction<?, ?, ?> adaptMapUsingContextFn(@Nonnull DistributedBiFunction mapFn) {
        return mapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedPredicate<?> adaptFilterFn(@Nonnull DistributedPredicate filterFn) {
        return filterFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, T> DistributedBiPredicate<C, T> adaptFilterUsingContextFn(@Nonnull DistributedBiPredicate<C, T> filterFn) {
        return filterFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <R, T> DistributedFunction<Object, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (DistributedFunction<? super Object, ? extends Traverser<?>>) flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, R, T> DistributedBiFunction<? super C, Object, ? extends Traverser<?>> adaptFlatMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (DistributedBiFunction<? super C, ? super Object, ? extends Traverser<?>>) flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    DistributedFunction<?, ?> adaptToStringFn(@Nonnull DistributedFunction<?, ? extends CharSequence> mapFn) {
        return mapFn;
    }

    @Nonnull
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return joinClause;
    }

    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adaptHashJoinOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (DistributedBiFunction<Object, T1, Object>) mapToOutputFn;
    }

    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adaptHashJoinOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (DistributedTriFunction<Object, T1, T2, Object>) mapToOutputFn;
    }

    <R, OUT> WindowResultFunction adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        return windowResultFn;
    }

    <K, R, OUT> KeyedWindowResultFunction adaptKeyedWindowResultFn(
            KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> keyedWindowResultFn
    ) {
        return keyedWindowResultFn;
    }

    @Nonnull
    public static ProcessorMetaSupplier adaptingMetaSupplier(ProcessorMetaSupplier metaSup, int[] ordinalsToAdapt) {
        return new WrappingProcessorMetaSupplier(metaSup, p -> new AdaptingProcessor(p, ordinalsToAdapt));
    }

    private static final class AdaptingProcessor extends ProcessorWrapper {
        private final AdaptingInbox adaptingInbox = new AdaptingInbox();
        private final BitSet shouldAdaptOrdinal = new BitSet();

        AdaptingProcessor(Processor wrapped, int[] ordinalsToAdapt) {
            super(wrapped);
            for (int ordinal : ordinalsToAdapt) {
                shouldAdaptOrdinal.set(ordinal);
            }
        }

        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            Inbox inboxToUse;
            if (shouldAdaptOrdinal.get(ordinal)) {
                inboxToUse = adaptingInbox;
                adaptingInbox.setWrappedInbox(inbox);
            } else {
                inboxToUse = inbox;
            }
            super.process(ordinal, inboxToUse);
        }
    }

    private static final class AdaptingInbox implements Inbox {
        private Inbox wrapped;

        void setWrappedInbox(@Nonnull Inbox wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public boolean isEmpty() {
            return wrapped.isEmpty();
        }

        @Override
        public Object peek() {
            return unwrapPayload(wrapped.peek());
        }

        @Override
        public Object poll() {
            return unwrapPayload(wrapped.poll());
        }

        @Override
        public void remove() {
            wrapped.remove();
        }

        private static Object unwrapPayload(Object jetEvent) {
            return jetEvent != null ? ((JetEvent) jetEvent).payload() : null;
        }
    }
}

class JetEventFunctionAdapter extends FunctionAdapter {
    @Nonnull @Override
    DistributedFunction adaptKeyFn(@Nonnull DistributedFunction keyFn) {
        return e -> keyFn.apply(((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedFunction adaptMapFn(@Nonnull DistributedFunction mapFn) {
        return e -> {
            Object result = mapFn.apply(((JetEvent) e).payload());
            return result != null ? jetEvent(result, ((JetEvent) e).timestamp()) : null;
        };
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedBiFunction adaptMapUsingContextFn(@Nonnull DistributedBiFunction mapFn) {
        return (context, e) -> {
            Object result = mapFn.apply(context, ((JetEvent) e).payload());
            return result != null ? jetEvent(result, ((JetEvent) e).timestamp()) : null;
        };
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedPredicate adaptFilterFn(@Nonnull DistributedPredicate filterFn) {
        return e -> filterFn.test(((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    DistributedBiPredicate adaptFilterUsingContextFn(@Nonnull DistributedBiPredicate filterFn) {
        return (context, e) -> filterFn.test(context, ((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, T> DistributedFunction<? super Object, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        DistributedFunction<Object, Traverser> fn = (DistributedFunction<Object, Traverser>) (Function) flatMapFn;
        return e -> fn.apply(((JetEvent) e).payload()).map(r -> jetEvent(r, ((JetEvent) e).timestamp()));
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <C, R, T> DistributedBiFunction<? super C, Object, ? extends Traverser<?>> adaptFlatMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        DistributedBiFunction<C, Object, Traverser> fn =
                (DistributedBiFunction<C, Object, Traverser>) (BiFunction) flatMapFn;
        return (context, e) -> fn.apply(context, ((JetEvent) e).payload())
                                 .map(r -> jetEvent(r, ((JetEvent) e).timestamp()));
    }

    @Nonnull @Override
    DistributedFunction<?, ?> adaptToStringFn(@Nonnull DistributedFunction mapFn) {
        return e -> mapFn.apply(((JetEvent) e).payload());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public JoinClause adaptJoinClause(@Nonnull JoinClause joinClause) {
        return onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<Object, T1, Object> adaptHashJoinOutputFn(
            DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return (e, t1) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(mapToOutputFn.apply(jetEvent.payload(), t1), jetEvent.timestamp());
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<Object, T1, T2, Object> adaptHashJoinOutputFn(
            DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return (e, t1, t2) -> {
            JetEvent<T> jetEvent = (JetEvent) e;
            return jetEvent(mapToOutputFn.apply(jetEvent.payload(), t1, t2), jetEvent.timestamp());
        };
    }

    @Override
    <R, OUT> WindowResultFunction<? super R, JetEvent<OUT>> adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        return (long winStart, long winEnd, R windowResult) ->
                jetEvent(windowResultFn.apply(winStart, winEnd, windowResult), winEnd);
    }

    @Override
    <K, R, OUT> KeyedWindowResultFunction<? super K, ? super R, JetEvent<OUT>> adaptKeyedWindowResultFn(
            KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> keyedWindowResultFn
    ) {
        return (long winStart, long winEnd, K key, R windowResult) ->
                jetEvent(keyedWindowResultFn.apply(winStart, winEnd, key, windowResult), winEnd);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static AggregateOperation adaptAggregateOperation(@Nonnull AggregateOperation aggrOp) {
        if (aggrOp instanceof AggregateOperation1) {
            return adaptAggregateOperation1((AggregateOperation1) aggrOp);
        } else if (aggrOp instanceof AggregateOperation2) {
            return adaptAggregateOperation2((AggregateOperation2) aggrOp);
        } else if (aggrOp instanceof AggregateOperation3) {
            return adaptAggregateOperation3((AggregateOperation3) aggrOp);
        } else {
            DistributedBiConsumer[] adaptedAccFns = new DistributedBiConsumer[aggrOp.arity()];
            Arrays.setAll(adaptedAccFns, i -> adaptAccumulateFn(aggrOp.accumulateFn(i)));
            return aggrOp.withAccumulateFns(adaptedAccFns);
        }
    }

    static <T, A, R> AggregateOperation1<JetEvent<T>, A, R> adaptAggregateOperation1(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggrOp.withAccumulateFn(adaptAccumulateFn(aggrOp.accumulateFn()));
    }

    static <T0, T1, A, R> AggregateOperation2<JetEvent<T0>, JetEvent<T1>, A, R> adaptAggregateOperation2(
            @Nonnull AggregateOperation2<? super T0, ? super T1, A, R> aggrOp
    ) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()));
    }

    static
    <T0, T1, T2, A, R> AggregateOperation3<JetEvent<T0>, JetEvent<T1>, JetEvent<T2>, A, R> adaptAggregateOperation3(
            @Nonnull AggregateOperation3<? super T0, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .<JetEvent<T1>>withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()))
                .withAccumulateFn2(adaptAccumulateFn(aggrOp.accumulateFn2()));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static <A, T> DistributedBiConsumer<? super A, ? super JetEvent<T>> adaptAccumulateFn(
            @Nonnull DistributedBiConsumer<? super A, ? super T> accumulateFn
    ) {
        return (A acc, JetEvent<T> t) -> accumulateFn.accept(acc, t.payload());
    }
}

