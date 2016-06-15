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
import com.hazelcast.jet.stream.impl.distributed.LongSummaryStatistics;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.terminal.Reducer;

import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public class LongPipeline implements DistributedLongStream {

    private final StreamContext context;
    private final Pipeline<Long> inner;

    public LongPipeline(StreamContext context, Pipeline<Long> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedLongStream filter(Distributed.LongPredicate predicate) {
        DistributedStream<Long> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedLongStream map(Distributed.LongUnaryOperator mapper) {
        DistributedStream<Long> map = inner.map(mapper::applyAsLong);
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(Distributed.LongFunction<? extends U> mapper) {
        return inner.map(m -> mapper.apply(m));
    }

    @Override
    public DistributedIntStream mapToInt(Distributed.LongToIntFunction mapper) {
        DistributedStream<Integer> stream = inner.map(mapper::applyAsInt);
        return new IntPipeline(context, (Pipeline<Integer>) stream);
    }

    @Override
    public DistributedDoubleStream mapToDouble(Distributed.LongToDoubleFunction mapper) {
        DistributedStream<Double> stream = inner.map(mapper::applyAsDouble);
        return new DoublePipeline(context, (Pipeline<Double>) stream);
    }

    @Override
    public DistributedLongStream flatMap(Distributed.LongFunction<? extends LongStream> mapper) {
        return wrap(inner.<Long>flatMap(n -> mapper.apply(n).boxed()));
    }

    @Override
    public DistributedLongStream distinct() {
        return wrap(inner.distinct());
    }

    @Override
    public DistributedLongStream sorted() {
        return wrap(inner.sorted());
    }

    @Override
    public DistributedLongStream peek(Distributed.LongConsumer action) {
        return wrap(inner.peek(action::accept));
    }

    @Override
    public DistributedLongStream limit(long maxSize) {
        return wrap(inner.limit(maxSize));
    }

    @Override
    public DistributedLongStream skip(long n) {
        return wrap(inner.skip(n));
    }

    @Override
    public void forEach(Distributed.LongConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(Distributed.LongConsumer action) {
        inner.forEachOrdered(action::accept);
    }

    @Override
    public long[] toArray() {
        IList<Long> list = inner.collect(DistributedCollectors.toIList());
        long[] array = new long[list.size()];

        Iterator<Long> iterator = list.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            array[index++] = iterator.next();
        }
        return array;
    }

    @Override
    public long reduce(long identity, Distributed.LongBinaryOperator op) {
        return new Reducer(context).<Long>reduce(inner,
                identity,
                (Distributed.BinaryOperator<Long>) op::applyAsLong);
    }

    @Override
    public OptionalLong reduce(Distributed.LongBinaryOperator op) {
        Optional<Long> result = new Reducer(context).reduce(inner,
                (Distributed.BinaryOperator<Long>) op::applyAsLong);
        return result.isPresent() ? OptionalLong.of(result.get()) : OptionalLong.empty();
    }

    @Override
    public <R> R collect(Distributed.Supplier<R> supplier,
                         Distributed.ObjLongConsumer<R> accumulator,
                         Distributed.BiConsumer<R, R> combiner) {
        Distributed.BiConsumer<R, Long> boxedAccumulator = accumulator::accept;
        return inner.collect(supplier, boxedAccumulator, combiner);
    }

    @Override
    public long sum() {
        return inner.reduce(0L, (a, b) -> a + b);
    }

    @Override
    public OptionalLong min() {
        return toOptionalLong(inner.min(Distributed.Comparator.naturalOrder()));
    }

    @Override
    public OptionalLong max() {
        return toOptionalLong(inner.max(Distributed.Comparator.naturalOrder()));
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
    public java.util.LongSummaryStatistics summaryStatistics() {
        return collect(LongSummaryStatistics::new, LongSummaryStatistics::accept,
                LongSummaryStatistics::combine);
    }

    @Override
    public boolean anyMatch(Distributed.LongPredicate predicate) {
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(Distributed.LongPredicate predicate) {
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(Distributed.LongPredicate predicate) {
        return inner.noneMatch(predicate::test);
    }

    @Override
    public OptionalLong findFirst() {
        return toOptionalLong(inner.findFirst());
    }

    @Override
    public OptionalLong findAny() {
        return toOptionalLong(inner.findAny());
    }


    @Override
    public DistributedDoubleStream asDoubleStream() {
        return mapToDouble(m -> (double) m);
    }

    @Override
    public DistributedStream<Long> boxed() {
        return inner;
    }

    @Override
    public DistributedLongStream sequential() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedLongStream parallel() {
        return this;
    }

    @Override
    public DistributedLongStream unordered() {
        return wrap(inner.unordered());
    }

    @Override
    public DistributedLongStream onClose(Runnable closeHandler) {
        return wrap(inner.onClose(closeHandler));
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        final Iterator<Long> iterator = inner.iterator();
        return new PrimitiveIterator.OfLong() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public long nextLong() {
                return iterator.next();
            }
        };
    }

    @Override
    public Spliterator.OfLong spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isParallel() {
        return inner.isParallel();
    }

    private DistributedLongStream wrap(Stream<Long> pipeline) {
        return new LongPipeline(context, (Pipeline<Long>) pipeline);
    }

    private static OptionalLong toOptionalLong(Optional<Long> optional) {
        return optional.isPresent() ? OptionalLong.of(optional.get()) : OptionalLong.empty();
    }
}
