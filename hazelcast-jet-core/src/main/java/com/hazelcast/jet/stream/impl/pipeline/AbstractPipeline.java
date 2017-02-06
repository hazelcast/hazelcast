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
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedDoubleStream;
import com.hazelcast.jet.stream.DistributedIntStream;
import com.hazelcast.jet.stream.DistributedLongStream;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.StreamUtil;
import com.hazelcast.jet.stream.impl.collectors.CustomStreamCollector;
import com.hazelcast.jet.stream.impl.terminal.Matcher;
import com.hazelcast.jet.stream.impl.terminal.Reducer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings(value = {"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public abstract class AbstractPipeline<E_OUT> implements Pipeline<E_OUT> {

    protected final StreamContext context;
    private final boolean isOrdered;

    public AbstractPipeline(StreamContext context) {
        this(context, false);
    }

    public AbstractPipeline(StreamContext context, boolean isOrdered) {
        this.context = context;
        this.isOrdered = isOrdered;
    }

    @Override
    public DistributedStream<E_OUT> filter(Predicate<? super E_OUT> predicate) {
        StreamUtil.checkSerializable(predicate, "predicate");
        return  new TransformPipeline(context, this,
                new TransformOperation(TransformOperation.Type.FILTER, predicate));
    }

    @Override
    public <R> DistributedStream<R> map(Function<? super E_OUT, ? extends R> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        return new TransformPipeline(context, this,
                new TransformOperation(TransformOperation.Type.MAP, mapper));
    }

    @Override
    public DistributedIntStream mapToInt(ToIntFunction<? super E_OUT> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Integer> map = (Pipeline<Integer>) this.map(mapper::applyAsInt);
        return new IntPipeline(context, map);
    }

    @Override
    public DistributedLongStream mapToLong(ToLongFunction<? super E_OUT> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Long> map = (Pipeline<Long>) this.map(mapper::applyAsLong);
        return new LongPipeline(context, map);
    }

    @Override
    public DistributedDoubleStream mapToDouble(ToDoubleFunction<? super E_OUT> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Double> map = (Pipeline<Double>) this.map(mapper::applyAsDouble);
        return new DoublePipeline(context, map);
    }

    @Override
    public <R> DistributedStream<R> flatMap(Function<? super E_OUT, ? extends Stream<? extends R>> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        return new TransformPipeline(context, this,
                new TransformOperation(TransformOperation.Type.FLAT_MAP, mapper));
    }

    @Override
    public DistributedIntStream flatMapToInt(Function<? super E_OUT, ? extends IntStream> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Integer> pipeline = (Pipeline<Integer>) this.flatMap(m -> mapper.apply(m).boxed());
        return new IntPipeline(context, pipeline);
    }

    @Override
    public DistributedLongStream flatMapToLong(Function<? super E_OUT, ? extends LongStream> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Long> pipeline = (Pipeline<Long>) this.flatMap(m -> mapper.apply(m).boxed());
        return new LongPipeline(context, pipeline);
    }

    @Override
    public DistributedDoubleStream flatMapToDouble(Function<? super E_OUT, ? extends DoubleStream> mapper) {
        StreamUtil.checkSerializable(mapper, "mapper");
        Pipeline<Double> pipeline = (Pipeline<Double>) this.flatMap(m -> mapper.apply(m).boxed());
        return new DoublePipeline(context, pipeline);
    }

    @Override
    public DistributedStream<E_OUT> distinct() {
        return new DistinctPipeline<>(context, this);
    }

    @Override
    public DistributedStream<E_OUT> sorted() {
        return sorted(null);
    }

    @Override
    public final DistributedStream<E_OUT> sorted(Comparator<? super E_OUT> comparator) {
        StreamUtil.checkSerializable(comparator, "comparator");
        return new SortPipeline<>(this, context, comparator);
    }

    @Override
    public DistributedStream<E_OUT> peek(Consumer<? super E_OUT> action) {
        StreamUtil.checkSerializable(action, "action");
        return new PeekPipeline<>(context, this, action);
    }

    @Override
    public DistributedStream<E_OUT> limit(long maxSize) {
        return new LimitPipeline<>(context, this, maxSize);
    }

    @Override
    public DistributedStream<E_OUT> skip(long n) {
        return new SkipPipeline<>(context, this, n);
    }

    @Override
    public void forEach(Consumer<? super E_OUT> action) {
        StreamUtil.checkSerializable(action, "action");
        IList<E_OUT> list = this.collect(DistributedCollectors.toIList());
        list.forEach(action::accept);
        list.destroy();
    }


    @Override
    public void forEachOrdered(Consumer<? super E_OUT> action) {
        StreamUtil.checkSerializable(action, "action");
        forEach(action);
    }

    @Override
    public Object[] toArray() {
        IList<E_OUT> list = collect(DistributedCollectors.toIList());
        Object[] array = list.toArray();
        list.destroy();
        return array;
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        IList<E_OUT> list = collect(DistributedCollectors.toIList());
        A[] array = generator.apply(list.size());
        array = list.toArray(array);
        list.destroy();
        return array;
    }

    @Override
    public E_OUT reduce(E_OUT identity, BinaryOperator<E_OUT> accumulator) {
        return new Reducer(context).reduce(this, identity, accumulator);
    }

    @Override
    public Optional<E_OUT> reduce(BinaryOperator<E_OUT> accumulator) {
        return new Reducer(context).reduce(this, accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super E_OUT, U> accumulator,
                        BinaryOperator<U> combiner) {
        return new Reducer(context).reduce(this, identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super E_OUT> accumulator,
                         BiConsumer<R, R> combiner) {
        StreamUtil.checkSerializable(supplier, "supplier");
        StreamUtil.checkSerializable(accumulator, "accumulator");
        StreamUtil.checkSerializable(combiner, "combiner");
        return new CustomStreamCollector<E_OUT, R>(supplier, accumulator, combiner).collect(context, this);
    }

    @Override
    public <R, A> R collect(DistributedCollector<? super E_OUT, A, R> collector) {
        StreamUtil.checkSerializable(collector, "collector");
        return collector.collect(context, this);
    }

    @Override
    public Optional<E_OUT> min(Comparator<? super E_OUT> comparator) {
        StreamUtil.checkSerializable(comparator, "comparator");
        return reduce((left, right) -> comparator.compare(left, right) < 0 ? left : right);
    }

    @Override
    public Optional<E_OUT> max(Comparator<? super E_OUT> comparator) {
        StreamUtil.checkSerializable(comparator, "comparator");
        return reduce((left, right) -> comparator.compare(left, right) > 0 ? left : right);
    }

    @Override
    public long count() {
        return reduce(0L,
                (i, m) -> i + 1,
                (a, b) -> a + b);
    }

    @Override
    public boolean anyMatch(Predicate<? super E_OUT> predicate) {
        StreamUtil.checkSerializable(predicate, "predicate");
        return new Matcher(context).anyMatch(this, predicate);
    }

    @Override
    public boolean allMatch(Predicate<? super E_OUT> predicate) {
        StreamUtil.checkSerializable(predicate, "predicate");
        return !new Matcher(context).anyMatch(this, t -> ! predicate.test(t));
    }

    @Override
    public boolean noneMatch(Predicate<? super E_OUT> predicate) {
        StreamUtil.checkSerializable(predicate, "predicate");
        return !new Matcher(context).anyMatch(this, predicate);
    }

    @Override
    public Optional<E_OUT> findFirst() {
        IList<E_OUT> first = this.limit(1).collect(DistributedCollectors.toIList());
        Optional<E_OUT> value = first.size() == 0 ? Optional.empty() : Optional.of(first.get(0));
        first.destroy();
        return value;
    }

    @Override
    public Optional<E_OUT> findAny() {
        return findFirst();
    }

    @Override
    public Iterator<E_OUT> iterator() {
        IList<E_OUT> list = collect(DistributedCollectors.toIList());
        Iterator<E_OUT> iterator = list.iterator();
        list.destroy();
        return iterator;
    }

    @Override
    public boolean isParallel() {
        return true;
    }

    @Override
    public DistributedStream<E_OUT> sequential() {
        throw new UnsupportedOperationException("Sequential streams are not supported for Hazelcast Jet");
    }

    @Override
    public DistributedStream<E_OUT> parallel() {
        return this;
    }

    @Override
    public DistributedStream<E_OUT> unordered() {
        if (isOrdered()) {
            return new UnorderedPipeline<>(context, this);
        }
        return this;
    }

    @Override
    public DistributedStream<E_OUT> onClose(Runnable closeHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Jet streams are not closeable.");
    }

    /**
     * @return if this step in the pipeline is ordered
     */
    @Override
    public boolean isOrdered() {
        return isOrdered;
    }

    @Override
    public Spliterator<E_OUT> spliterator() {
        throw new UnsupportedOperationException();
    }

}
