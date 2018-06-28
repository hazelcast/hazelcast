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

import static com.hazelcast.jet.impl.pipeline.JetEvent.jetEvent;


public class FunctionAdapter {

    @Nonnull
    <T, K> DistributedFunction<?, ? extends K> adaptKeyFn(@Nonnull DistributedFunction<? super T, ? extends K> keyFn) {
        return keyFn;
    }

    @Nonnull
    <T, R> DistributedFunction<?, ?> adaptMapFn(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return mapFn;
    }

    @Nonnull
    <T> DistributedPredicate<?> adaptFilterFn(@Nonnull DistributedPredicate<? super T> filterFn) {
        return filterFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <T, R> DistributedFunction<?, ? extends Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, T, R> DistributedBiFunction<? super C, ?, ?> adaptMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        return mapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, T> DistributedBiPredicate<? super C, ?> adaptFilterUsingContextFn(
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        return filterFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <C, T, R> DistributedBiFunction<? super C, ?, ? extends Traverser<?>> adaptFlatMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return flatMapFn;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    <T, R extends CharSequence> DistributedFunction<?, ? extends R> adaptToStringFn(
            @Nonnull DistributedFunction<? super T, ? extends R> toStringFn
    ) {
        return toStringFn;
    }

    @Nonnull
    public <K, T0, T1, T1_OUT> JoinClause<? extends K, ?, ? super T1, ? extends T1_OUT>
    adaptJoinClause(@Nonnull JoinClause<? extends K, ? super T0, ? super T1, ? extends T1_OUT> joinClause) {
        return joinClause;
    }

    @SuppressWarnings("unchecked")
    public <T, T1, R> DistributedBiFunction<?, ? super T1, ?> adaptHashJoinOutputFn(
            DistributedBiFunction<? super T, ? super T1, ? extends R> mapToOutputFn
    ) {
        return mapToOutputFn;
    }

    @SuppressWarnings("unchecked")
    <T, T1, T2, R> DistributedTriFunction<?, ? super T1, ? super T2, ?> adaptHashJoinOutputFn(
            DistributedTriFunction<? super T, ? super T1, ? super T2, ? extends R> mapToOutputFn
    ) {
        return mapToOutputFn;
    }

    <R, OUT> WindowResultFunction<?, ?> adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        return windowResultFn;
    }

    <K, R, OUT> KeyedWindowResultFunction<?, ?, ?> adaptKeyedWindowResultFn(
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
    <T, K> DistributedFunction<? super JetEvent<T>, ? extends K> adaptKeyFn(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        return e -> keyFn.apply(e.payload());
    }

    @Nonnull @Override
    <T, R> DistributedFunction<? super JetEvent<T>, ?> adaptMapFn(
            @Nonnull DistributedFunction<? super T, ? extends R> mapFn
    ) {
        return e -> jetEvent(mapFn.apply(e.payload()), e.timestamp());
    }

    @Nonnull @Override
    <T> DistributedPredicate<? super JetEvent<T>> adaptFilterFn(@Nonnull DistributedPredicate<? super T> filterFn) {
        return e -> filterFn.test(e.payload());
    }

    @Nonnull @Override
    <T, R> DistributedFunction<? super JetEvent<T>, Traverser<?>> adaptFlatMapFn(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return e -> flatMapFn.apply(e.payload()).map(r -> jetEvent(r, e.timestamp()));
    }

    @Nonnull @Override
    <C, T, R> DistributedBiFunction<? super C, ? super JetEvent<T>, ? extends JetEvent<R>> adaptMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        return (context, e) -> jetEvent(mapFn.apply(context, e.payload()), e.timestamp());
    }

    @Nonnull @Override
    <C, T> DistributedBiPredicate<? super C, ? super JetEvent<T>> adaptFilterUsingContextFn(
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        return (context, e) -> filterFn.test(context, e.payload());
    }

    @Nonnull @Override
    <C, T, R> DistributedBiFunction<? super C, ? super JetEvent<T>, ? extends Traverser<JetEvent<R>>>
    adaptFlatMapUsingContextFn(
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return (context, e) -> flatMapFn.apply(context, e.payload()).map(r -> jetEvent(r, e.timestamp()));
    }

    @Nonnull @Override
    <T, STR extends CharSequence> DistributedFunction<? super JetEvent<T>, ? extends STR> adaptToStringFn(
            @Nonnull DistributedFunction<? super T, ? extends STR> toStringFn
    ) {
        return e -> toStringFn.apply(e.payload());
    }

    @Nonnull @Override
    public <K, T0, T1, T1_OUT> JoinClause<? extends K, ? super JetEvent<T0>, ? super T1, ? extends T1_OUT>
    adaptJoinClause(
            @Nonnull JoinClause<? extends K, ? super T0, ? super T1, ? extends T1_OUT> joinClause
    ) {
        return JoinClause.<K, JetEvent<T0>, T1>onKeys(adaptKeyFn(joinClause.leftKeyFn()), joinClause.rightKeyFn())
                .projecting(joinClause.rightProjectFn());
    }

    @Override
    public <T, T1, R> DistributedBiFunction<? super JetEvent<T>, ? super T1, ?> adaptHashJoinOutputFn(
            DistributedBiFunction<? super T, ? super T1, ? extends R> mapToOutputFn
    ) {
        return (e, t1) -> jetEvent(mapToOutputFn.apply(e.payload(), t1), e.timestamp());
    }

    @Override
    <T, T1, T2, R> DistributedTriFunction<? super JetEvent<T>, ? super T1, ? super T2, ?> adaptHashJoinOutputFn(
            DistributedTriFunction<? super T, ? super T1, ? super T2, ? extends R> mapToOutputFn
    ) {
        return (e, t1, t2) -> jetEvent(mapToOutputFn.apply(e.payload(), t1, t2), e.timestamp());
    }

    @Override
    <R, OUT> WindowResultFunction<? super R, ? extends JetEvent<OUT>> adaptWindowResultFn(
            WindowResultFunction<? super R, ? extends OUT> windowResultFn
    ) {
        // use `winEnd - 1` for jetEvent, see https://github.com/hazelcast/hazelcast-jet/issues/898
        return (winStart, winEnd, windowResult) ->
                jetEvent(windowResultFn.apply(winStart, winEnd, windowResult), winEnd - 1);
    }

    @Override
    <K, R, OUT> KeyedWindowResultFunction<? super K, ? super R, ? extends JetEvent<OUT>> adaptKeyedWindowResultFn(
            KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> keyedWindowResultFn
    ) {
        // use `winEnd - 1` for jetEvent, see https://github.com/hazelcast/hazelcast-jet/issues/898
        return (winStart, winEnd, key, windowResult) ->
                jetEvent(keyedWindowResultFn.apply(winStart, winEnd, key, windowResult), winEnd - 1);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A, R> AggregateOperation<A, ? extends R> adaptAggregateOperation(
            @Nonnull AggregateOperation<A, ? extends R> aggrOp
    ) {
        if (aggrOp instanceof AggregateOperation1) {
            return adaptAggregateOperation1((AggregateOperation1) aggrOp);
        } else if (aggrOp instanceof AggregateOperation2) {
            return adaptAggregateOperation2((AggregateOperation2) aggrOp);
        } else if (aggrOp instanceof AggregateOperation3) {
            return adaptAggregateOperation3((AggregateOperation3) aggrOp);
        } else {
            DistributedBiConsumer[] adaptedAccFns = new DistributedBiConsumer[aggrOp.arity()];
            Arrays.setAll(adaptedAccFns, i -> adaptAccumulateFn((DistributedBiConsumer) aggrOp.accumulateFn(i)));
            return aggrOp.withAccumulateFns(adaptedAccFns);
        }
    }

    static <T, A, R> AggregateOperation1<? super JetEvent<T>, A, ? extends R> adaptAggregateOperation1(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return aggrOp.withAccumulateFn(adaptAccumulateFn(aggrOp.accumulateFn()));
    }

    static <T0, T1, A, R> AggregateOperation2<? super JetEvent<T0>, ? super JetEvent<T1>, A, ? extends R>
    adaptAggregateOperation2(@Nonnull AggregateOperation2<? super T0, ? super T1, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()));
    }

    static <T0, T1, T2, A, R>
    AggregateOperation3<? super JetEvent<T0>, ? super JetEvent<T1>, ? super JetEvent<T2>, A, ? extends R>
    adaptAggregateOperation3(@Nonnull AggregateOperation3<? super T0, ? super T1, ? super T2, A, ? extends R> aggrOp) {
        return aggrOp
                .<JetEvent<T0>>withAccumulateFn0(adaptAccumulateFn(aggrOp.accumulateFn0()))
                .<JetEvent<T1>>withAccumulateFn1(adaptAccumulateFn(aggrOp.accumulateFn1()))
                .withAccumulateFn2(adaptAccumulateFn(aggrOp.accumulateFn2()));
    }

    @Nonnull
    private static <A, T> DistributedBiConsumer<? super A, ? super JetEvent<T>> adaptAccumulateFn(
            @Nonnull DistributedBiConsumer<? super A, ? super T> accumulateFn
    ) {
        return (acc, t) -> accumulateFn.accept(acc, t.payload());
    }
}

