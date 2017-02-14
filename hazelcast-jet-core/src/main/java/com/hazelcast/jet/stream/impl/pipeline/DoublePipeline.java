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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.distributed.DistributedDoubleSummaryStatistics;

import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.stream.impl.StreamUtil.checkSerializable;

@SuppressWarnings("checkstyle:methodcount")
class DoublePipeline implements DistributedDoubleStream {

    private final StreamContext context;
    private final Pipeline<Double> inner;

    DoublePipeline(StreamContext context, Pipeline<Double> inner) {
        this.context = context;
        this.inner = inner;
    }

    @Override
    public DistributedDoubleStream filter(DoublePredicate predicate) {
        checkSerializable(predicate, "predicate");
        DistributedStream<Double> filter = inner.filter(predicate::test);
        return wrap(filter);
    }

    @Override
    public DistributedDoubleStream map(DoubleUnaryOperator mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Double> map = inner.map(mapper::applyAsDouble);
        return wrap(map);
    }

    @Override
    public <U> DistributedStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
        checkSerializable(mapper, "mapper");
        return inner.map(mapper::apply);
    }

    @Override
    public DistributedIntStream mapToInt(DoubleToIntFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Integer> stream = inner.map(mapper::applyAsInt);
        return new IntPipeline(context, (Pipeline<Integer>) stream);
    }

    @Override
    public DistributedLongStream mapToLong(DoubleToLongFunction mapper) {
        checkSerializable(mapper, "mapper");
        DistributedStream<Long> stream = inner.map(mapper::applyAsLong);
        return new LongPipeline(context, (Pipeline<Long>) stream);
    }

    @Override
    public DistributedDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
        checkSerializable(mapper, "mapper");
        return wrap(inner.flatMap(n -> mapper.apply(n).boxed()));
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
    public DistributedDoubleStream peek(DoubleConsumer action) {
        checkSerializable(action, "action");
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
    public void forEach(DoubleConsumer action) {
        inner.forEach(action::accept);
    }

    @Override
    public void forEachOrdered(DoubleConsumer action) {
        checkSerializable(action, "action");
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
    public double reduce(double identity, DoubleBinaryOperator op) {
        checkSerializable(op, "op");

        return inner.reduce(identity, op::applyAsDouble);
    }

    @Override
    public OptionalDouble reduce(DoubleBinaryOperator op) {
        checkSerializable(op, "op");

        Optional<Double> result = inner.reduce(op::applyAsDouble);
        return result.isPresent() ? OptionalDouble.of(result.get()) : OptionalDouble.empty();
    }

    @Override
    public <R> R collect(Supplier<R> supplier,
                         ObjDoubleConsumer<R> accumulator,
                         BiConsumer<R, R> combiner) {
        checkSerializable(accumulator, "accumulator");
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
    public boolean anyMatch(DoublePredicate predicate) {
        checkSerializable(predicate, "predicate");
        return inner.anyMatch(predicate::test);
    }

    @Override
    public boolean allMatch(DoublePredicate predicate) {
        checkSerializable(predicate, "predicate");
        return inner.allMatch(predicate::test);
    }

    @Override
    public boolean noneMatch(DoublePredicate predicate) {
        checkSerializable(predicate, "predicate");
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

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static OptionalDouble toOptionalDouble(Optional<Double> optional) {
        return optional.isPresent() ? OptionalDouble.of(optional.get()) : OptionalDouble.empty();
    }
}
