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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.core.IList;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.distributed.DistributedLongSummaryStatistics;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

@SuppressWarnings("checkstyle:methodcount")
class LongPipe implements DistributedLongStream {

    private final StreamContext context;
    private final Pipe<Long> inner;

    LongPipe(StreamContext context, Pipe<Long> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedLongStream filter(LongPredicate predicate) {
        checkSerializable(predicate, "predicate");
        DistributedStream<Long> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedLongStream map(LongUnaryOperator mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Long> map = inner.map(mapper::applyAsLong);
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(LongFunction<? extends U> mapper) {
        checkSerializable(mapper, "mapper");
        return inner.map(mapper::apply);
    }

    @Override
    public DistributedIntStream mapToInt(LongToIntFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Integer> stream = inner.map(mapper::applyAsInt);
        return new IntPipeline(context, (Pipe<Integer>) stream);
    }

    @Override
    public DistributedDoubleStream mapToDouble(LongToDoubleFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Double> stream = inner.map(mapper::applyAsDouble);
        return new DoublePipeline(context, (Pipe<Double>) stream);
    }

    @Override
    public DistributedLongStream flatMap(LongFunction<? extends LongStream> mapper) {
        checkSerializable(mapper, "mapper");
        return wrap(inner.flatMap(n -> mapper.apply(n).boxed()));
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
    public DistributedLongStream peek(LongConsumer action) {
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
    public void forEach(LongConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(LongConsumer action) {
        inner.forEachOrdered(action::accept);
    }

    @Override
    public long[] toArray() {
        IList<Long> list = inner.collect(DistributedCollectors.toIList(uniqueListName()));
        try {
            long[] array = new long[list.size()];

            int index = 0;
            for (Long l : list) {
                array[index++] = l;
            }
            return array;
        } finally {
            list.destroy();
        }
    }

    @Override
    public long reduce(long identity, LongBinaryOperator op) {
        return inner.<Long>reduce(identity, op::applyAsLong);
    }

    @Override
    public OptionalLong reduce(LongBinaryOperator op) {
        Optional<Long> result = inner.reduce(op::applyAsLong);
        return result.isPresent() ? OptionalLong.of(result.get()) : OptionalLong.empty();
    }

    @Override
    public <R> R collect(Supplier<R> supplier,
                         ObjLongConsumer<R> accumulator,
                         BiConsumer<R, R> combiner) {
        DistributedBiConsumer<R, Long> boxedAccumulator = accumulator::accept;
        return inner.collect(supplier, boxedAccumulator, combiner);
    }

    @Override
    public long sum() {
        return inner.reduce(0L, (a, b) -> a + b);
    }

    @Override
    public OptionalLong min() {
        return toOptionalLong(inner.min(DistributedComparator.naturalOrder()));
    }

    @Override
    public OptionalLong max() {
        return toOptionalLong(inner.max(DistributedComparator.naturalOrder()));
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
        return collect(DistributedLongSummaryStatistics::new, DistributedLongSummaryStatistics::accept,
                DistributedLongSummaryStatistics::combine);
    }

    @Override
    public boolean anyMatch(LongPredicate predicate) {
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(LongPredicate predicate) {
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(LongPredicate predicate) {
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

    @Nonnull @Override
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

    @Override
    public DistributedLongStream configure(JobConfig jobConfig) {
        return wrap(inner.configure(jobConfig));
    }

    private DistributedLongStream wrap(Stream<Long> pipeline) {
        return new LongPipe(context, (Pipe<Long>) pipeline);
    }

    private static OptionalLong toOptionalLong(Optional<Long> optional) {
        return optional.isPresent() ? OptionalLong.of(optional.get()) : OptionalLong.empty();
    }
}
