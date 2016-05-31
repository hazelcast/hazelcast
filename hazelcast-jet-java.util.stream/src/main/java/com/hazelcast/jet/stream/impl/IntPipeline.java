/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.core.IList;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.terminal.Reducer;

import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public class IntPipeline implements DistributedIntStream {

    private final StreamContext context;
    private final Pipeline<Integer> inner;

    public IntPipeline(StreamContext context, Pipeline<Integer> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedIntStream filter(Distributed.IntPredicate predicate) {
        DistributedStream<Integer> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedIntStream map(Distributed.IntUnaryOperator mapper) {
        DistributedStream<Integer> map = inner.map(integer -> mapper.applyAsInt(integer));
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(Distributed.IntFunction<? extends U> mapper) {
        return inner.map(m -> mapper.apply(m));
    }

    @Override
    public DistributedLongStream mapToLong(Distributed.IntToLongFunction mapper) {
        DistributedStream<Long> stream = inner.map(mapper::applyAsLong);
        return new LongPipeline(context, (Pipeline<Long>) stream);
    }

    @Override
    public DistributedDoubleStream mapToDouble(Distributed.IntToDoubleFunction mapper) {
        DistributedStream<Double> stream = inner.map(mapper::applyAsDouble);
        return new DoublePipeline(context, (Pipeline<Double>) stream);
    }

    @Override
    public DistributedIntStream flatMap(Distributed.IntFunction<? extends IntStream> mapper) {
        return wrap(inner.<Integer>flatMap(n -> mapper.apply(n).boxed()));
    }

    @Override
    public DistributedIntStream distinct() {
        return wrap(inner.distinct());
    }

    @Override
    public DistributedIntStream sorted() {
        return wrap(inner.sorted());
    }

    @Override
    public DistributedIntStream peek(Distributed.IntConsumer action) {
        return wrap(inner.peek(action::accept));
    }

    @Override
    public DistributedIntStream limit(long maxSize) {
        return wrap(inner.limit(maxSize));
    }

    @Override
    public DistributedIntStream skip(long n) {
        return wrap(inner.skip(n));
    }

    @Override
    public void forEach(Distributed.IntConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(Distributed.IntConsumer action) {
        inner.forEachOrdered(action::accept);
    }

    @Override
    public int[] toArray() {
        IList<Integer> list = inner.collect(DistributedCollectors.toIList());
        int[] array = new int[list.size()];

        Iterator<Integer> iterator = list.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            array[index++] = iterator.next();
        }
        return array;
    }

    @Override
    public int reduce(int identity, Distributed.IntBinaryOperator op) {
        return new Reducer(context).<Integer>reduce(inner,
                identity,
                (Distributed.BinaryOperator<Integer>) op::applyAsInt);
    }

    @Override
    public OptionalInt reduce(Distributed.IntBinaryOperator op) {
        Optional<Integer> result = new Reducer(context).reduce(inner,
                (Distributed.BinaryOperator<Integer>) op::applyAsInt);
        return result.isPresent() ? OptionalInt.of(result.get()) : OptionalInt.empty();
    }

    @Override
    public <R> R collect(Distributed.Supplier<R> supplier,
                         Distributed.ObjIntConsumer<R> accumulator,
                         Distributed.BiConsumer<R, R> combiner) {
        Distributed.BiConsumer<R, Integer> boxedAccumulator = accumulator::accept;
        return inner.collect(supplier, boxedAccumulator, combiner);
    }

    @Override
    public int sum() {
        return inner.reduce(0, (a, b) -> a + b);
    }

    @Override
    public OptionalInt min() {
        return toOptionalInt(inner.min(Distributed.Comparator.naturalOrder()));
    }

    @Override
    public OptionalInt max() {
        return toOptionalInt(inner.max(Distributed.Comparator.naturalOrder()));
    }

    @Override
    public long count() {
        return inner.count();
    }

    @Override
    public OptionalDouble average() {
        long[] avg = collect(() -> new long[2],
                (ll, i) -> {
                    ll[0]++;
                    ll[1] += i;
                },
                (ll, rr) -> {
                    ll[0] += rr[0];
                    ll[1] += rr[1];
                });
        return avg[0] > 0
                ? OptionalDouble.of((double) avg[1] / avg[0])
                : OptionalDouble.empty();
    }

    @Override
    public IntSummaryStatistics summaryStatistics() {
        return collect(Distributed.IntSummaryStatistics::new, Distributed.IntSummaryStatistics::accept,
                Distributed.IntSummaryStatistics::combine);
    }

    @Override
    public boolean anyMatch(Distributed.IntPredicate predicate) {
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(Distributed.IntPredicate predicate) {
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(Distributed.IntPredicate predicate) {
        return inner.noneMatch(predicate::test);
    }

    @Override
    public OptionalInt findFirst() {
        return toOptionalInt(inner.findFirst());
    }

    @Override
    public OptionalInt findAny() {
        return toOptionalInt(inner.findAny());
    }

    @Override
    public DistributedLongStream asLongStream() {
        return mapToLong(m -> (long) m);
    }

    @Override
    public DistributedDoubleStream asDoubleStream() {
        return mapToDouble(m -> (double) m);
    }

    @Override
    public DistributedStream<Integer> boxed() {
        return inner;
    }

    @Override
    public DistributedIntStream sequential() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedIntStream parallel() {
        return this;
    }

    @Override
    public DistributedIntStream unordered() {
        return wrap(inner.unordered());
    }

    @Override
    public DistributedIntStream onClose(Runnable closeHandler) {
        return wrap(inner.onClose(closeHandler));
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        final Iterator<Integer> iterator = inner.iterator();
        return new PrimitiveIterator.OfInt() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public int nextInt() {
                return iterator.next();
            }
        };
    }

    @Override
    public Spliterator.OfInt spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isParallel() {
        return inner.isParallel();
    }

    private DistributedIntStream wrap(Stream<Integer> pipeline) {
        return new IntPipeline(context, (Pipeline<Integer>) pipeline);
    }

    private static OptionalInt toOptionalInt(Optional<Integer> optional) {
        return optional.isPresent() ? OptionalInt.of(optional.get()) : OptionalInt.empty();
    }
}
