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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.distributed.DistributedIntSummaryStatistics;

import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

@SuppressWarnings("checkstyle:methodcount")
class IntPipeline implements DistributedIntStream {

    private final StreamContext context;
    private final Pipeline<Integer> inner;

    IntPipeline(StreamContext context, Pipeline<Integer> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedIntStream filter(IntPredicate predicate) {
        checkSerializable(predicate, "predicate");
        DistributedStream<Integer> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedIntStream map(IntUnaryOperator mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Integer> map = inner.map(mapper::applyAsInt);
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(IntFunction<? extends U> mapper) {
        checkSerializable(mapper, "mapper");
        return inner.map(mapper::apply);
    }

    @Override
    public DistributedLongStream mapToLong(IntToLongFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Long> stream = inner.map(mapper::applyAsLong);
        return new LongPipeline(context, (Pipeline<Long>) stream);
    }

    @Override
    public DistributedDoubleStream mapToDouble(IntToDoubleFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Double> stream = inner.map(mapper::applyAsDouble);
        return new DoublePipeline(context, (Pipeline<Double>) stream);
    }

    @Override
    public DistributedIntStream flatMap(IntFunction<? extends IntStream> mapper) {
        checkSerializable(mapper, "mapper");
        return wrap(inner.flatMap(n -> mapper.apply(n).boxed()));
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
    public DistributedIntStream peek(IntConsumer action) {
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
    public void forEach(IntConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(IntConsumer action) {
        inner.forEachOrdered(action::accept);
    }

    @Override
    public int[] toArray() {
        IList<Integer> list = inner.collect(DistributedCollectors.toIList(uniqueListName()));
        try {
            int[] array = new int[list.size()];

            int index = 0;
            for (Integer i : list) {
                array[index++] = i;
            }
            return array;
        } finally {
            list.destroy();
        }
    }

    @Override
    public int reduce(int identity, IntBinaryOperator op) {
        return inner.reduce(identity, op::applyAsInt);
    }

    @Override
    public OptionalInt reduce(IntBinaryOperator op) {
        Optional<Integer> result = inner.reduce(op::applyAsInt);
        return result.isPresent() ? OptionalInt.of(result.get()) : OptionalInt.empty();
    }

    @Override
    public <R> R collect(Supplier<R> supplier,
                         ObjIntConsumer<R> accumulator,
                         BiConsumer<R, R> combiner) {
        DistributedBiConsumer<R, Integer> boxedAccumulator = accumulator::accept;
        return inner.collect(supplier, boxedAccumulator, combiner);
    }

    @Override
    public int sum() {
        return inner.reduce(0, (a, b) -> a + b);
    }

    @Override
    public OptionalInt min() {
        return toOptionalInt(inner.min(DistributedComparator.naturalOrder()));
    }

    @Override
    public OptionalInt max() {
        return toOptionalInt(inner.max(DistributedComparator.naturalOrder()));
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
    public java.util.IntSummaryStatistics summaryStatistics() {
        return collect(DistributedIntSummaryStatistics::new, DistributedIntSummaryStatistics::accept,
                DistributedIntSummaryStatistics::combine);
    }

    @Override
    public boolean anyMatch(IntPredicate predicate) {
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(IntPredicate predicate) {
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(IntPredicate predicate) {
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

    @Override
    public DistributedIntStream configure(JobConfig jobConfig) {
        return wrap(inner.configure(jobConfig));
    }

    private DistributedIntStream wrap(Stream<Integer> pipeline) {
        return new IntPipeline(context, (Pipeline<Integer>) pipeline);
    }

    private static OptionalInt toOptionalInt(Optional<Integer> optional) {
        return optional.isPresent() ? OptionalInt.of(optional.get()) : OptionalInt.empty();
    }
}
