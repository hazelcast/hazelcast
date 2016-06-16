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
import com.hazelcast.jet.stream.impl.distributed.DistributedDoubleSummaryStatistics;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.terminal.Reducer;

import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:methodcount")
public class DoublePipeline implements DistributedDoubleStream {

    private final StreamContext context;
    private final Pipeline<Double> inner;

    public DoublePipeline(StreamContext context, Pipeline<Double> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedDoubleStream filter(Distributed.DoublePredicate predicate) {
        DistributedStream<Double> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedDoubleStream map(Distributed.DoubleUnaryOperator mapper) {
        DistributedStream<Double> map = inner.map(mapper::applyAsDouble);
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(Distributed.DoubleFunction<? extends U> mapper) {
        return inner.map(m -> mapper.apply(m));
    }

    @Override
    public DistributedIntStream mapToInt(Distributed.DoubleToIntFunction mapper) {
        DistributedStream<Integer> stream = inner.map(mapper::applyAsInt);
        return new IntPipeline(context, (Pipeline<Integer>) stream);
    }

    @Override
    public DistributedLongStream mapToLong(Distributed.DoubleToLongFunction mapper) {
        DistributedStream<Long> stream = inner.map(mapper::applyAsLong);
        return new LongPipeline(context, (Pipeline<Long>) stream);
    }

    @Override
    public DistributedDoubleStream flatMap(Distributed.DoubleFunction<? extends DoubleStream> mapper) {
        return wrap(inner.<Double>flatMap(n -> mapper.apply(n).boxed()));
    }

    @Override
    public DistributedDoubleStream distinct() {
        return wrap(inner.distinct());
    }

    @Override
    public DistributedDoubleStream sorted() {
        return wrap(inner.sorted());
    }

    @Override
    public DistributedDoubleStream peek(Distributed.DoubleConsumer action) {
        return wrap(inner.peek(action::accept));
    }

    @Override
    public DistributedDoubleStream limit(long maxSize) {
        return wrap(inner.limit(maxSize));
    }

    @Override
    public DistributedDoubleStream skip(long n) {
        return wrap(inner.skip(n));
    }

    @Override
    public void forEach(Distributed.DoubleConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(Distributed.DoubleConsumer action) {
        inner.forEachOrdered(action::accept);
    }

    @Override
    public double[] toArray() {
        IList<Double> list = inner.collect(DistributedCollectors.toIList());
        double[] array = new double[list.size()];

        Iterator<Double> iterator = list.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            array[index++] = iterator.next();
        }
        return array;
    }

    @Override
    public double reduce(double identity, Distributed.DoubleBinaryOperator op) {
        return new Reducer(context).<Double>reduce(inner,
                identity,
                (Distributed.BinaryOperator<Double>) op::applyAsDouble);
    }

    @Override
    public OptionalDouble reduce(Distributed.DoubleBinaryOperator op) {
        Optional<Double> result = new Reducer(context).reduce(inner,
                (Distributed.BinaryOperator<Double>) op::applyAsDouble);
        return result.isPresent() ? OptionalDouble.of(result.get()) : OptionalDouble.empty();
    }

    @Override
    public <R> R collect(Distributed.Supplier<R> supplier,
                         Distributed.ObjDoubleConsumer<R> accumulator,
                         Distributed.BiConsumer<R, R> combiner) {
        Distributed.BiConsumer<R, Double> boxedAccumulator = accumulator::accept;
        return inner.collect(supplier, boxedAccumulator, combiner);
    }

    @Override
    public double sum() {
        return inner.reduce(0D, (a, b) -> a + b);
    }

    @Override
    public OptionalDouble min() {
        return toOptionalDouble(inner.min(Distributed.Comparator.naturalOrder()));
    }

    @Override
    public OptionalDouble max() {
        return toOptionalDouble(inner.max(Distributed.Comparator.naturalOrder()));
    }

    @Override
    public long count() {
        return inner.count();
    }

    @Override
    public OptionalDouble average() {
        double[] avg = collect(() -> new double[2],
                (ll, i) -> {
                    ll[0]++;
                    ll[1] += i;
                },
                (ll, rr) -> {
                    ll[0] += rr[0];
                    ll[1] += rr[1];
                });
        return avg[0] > 0
                ? OptionalDouble.of(avg[1] / avg[0])
                : OptionalDouble.empty();
    }

    @Override
    public java.util.DoubleSummaryStatistics summaryStatistics() {
        return collect(DistributedDoubleSummaryStatistics::new, DistributedDoubleSummaryStatistics::accept,
                DistributedDoubleSummaryStatistics::combine);
    }

    @Override
    public boolean anyMatch(Distributed.DoublePredicate predicate) {
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(Distributed.DoublePredicate predicate) {
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(Distributed.DoublePredicate predicate) {
        return inner.noneMatch(predicate::test);
    }

    @Override
    public OptionalDouble findFirst() {
        return toOptionalDouble(inner.findFirst());
    }

    @Override
    public OptionalDouble findAny() {
        return toOptionalDouble(inner.findAny());
    }

    @Override
    public DistributedStream<Double> boxed() {
        return inner;
    }

    @Override
    public DistributedDoubleStream sequential() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DistributedDoubleStream parallel() {
        return this;
    }

    @Override
    public DistributedDoubleStream unordered() {
        return wrap(inner.unordered());
    }

    @Override
    public DistributedDoubleStream onClose(Runnable closeHandler) {
        return wrap(inner.onClose(closeHandler));
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public PrimitiveIterator.OfDouble iterator() {
        final Iterator<Double> iterator = inner.iterator();
        return new PrimitiveIterator.OfDouble() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public double nextDouble() {
                return iterator.next();
            }
        };
    }

    @Override
    public Spliterator.OfDouble spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isParallel() {
        return inner.isParallel();
    }

    private DistributedDoubleStream wrap(Stream<Double> pipeline) {
        return new DoublePipeline(context, (Pipeline<Double>) pipeline);
    }

    private static OptionalDouble toOptionalDouble(Optional<Double> optional) {
        return optional.isPresent() ? OptionalDouble.of(optional.get()) : OptionalDouble.empty();
    }
}
