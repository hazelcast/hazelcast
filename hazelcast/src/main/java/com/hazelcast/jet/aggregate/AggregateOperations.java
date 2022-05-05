/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.aggregate;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BinaryOperatorEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToDoubleFunctionEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.accumulator.PickAnyAccumulator;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.aggregate.AggregateOpAggregator;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;

/**
 * Utility class with factory methods for several useful aggregate
 * operations. See the Javadoc on {@link AggregateOperation}. You can
 * also create your own aggregate operation using the {@link
 * AggregateOperation#withCreate builder object}.
 *
 * @since Jet 3.0
 */
public final class AggregateOperations {

    private AggregateOperations() {
    }

    /**
     * Returns an aggregate operation that counts the items it observes. The
     * result is of type {@code long}.
     * <p>
     * This sample takes a stream of words and finds the number of occurrences
     * of each word in it:
     * <pre>{@code
     * BatchStage<String> words = pipeline.readFrom(wordSource);
     * BatchStage<Entry<String, Long>> wordFrequencies =
     *     words.groupingKey(wholeItem()).aggregate(counting());
     * }</pre>
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongAccumulator, Long> counting() {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.add(1))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtractAllowingOverflow)
                .andExportFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code long}
     * values it obtains by applying {@code getLongValueFn} to each item.
     * <p>
     * This sample takes a stream of lines of text and outputs a single {@code
     * long} number telling how many words there were in the stream:
     * <pre>{@code
     * BatchStage<String> linesOfText = pipeline.readFrom(textSource);
     * BatchStage<Long> numberOfWordsInText =
     *         linesOfText
     *                 .map(line -> line.split("\\W+"))
     *                 .aggregate(summingLong(wordsInLine -> wordsInLine.length));
     * }</pre>
     *
     * <strong>Note:</strong> if the sum exceeds {@code Long.MAX_VALUE}, the job
     * will fail with an {@code ArithmeticException}.
     *
     * @param getLongValueFn function that extracts the {@code long} values you
     *     want to sum. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongAccumulator, Long> summingLong(
            @Nonnull ToLongFunctionEx<? super T> getLongValueFn
    ) {
        checkSerializable(getLongValueFn, "getLongValueFn");
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator a, T item) -> a.add(getLongValueFn.applyAsLong(item)))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtract)
                .andExportFinish(LongAccumulator::get);
    }

    /**
     * Returns an aggregate operation that computes the sum of the {@code
     * double} values it obtains by applying {@code getDoubleValueFn} to each
     * item.
     * <p>
     * This sample takes a stream of purchase events and outputs a single
     * {@code double} value that tells the total sum of money spent in
     * them:
     * <pre>{@code
     * BatchStage<Purchase> purchases = pipeline.readFrom(purchaseSource);
     * BatchStage<Double> purchaseVolume =
     *         purchases.aggregate(summingDouble(Purchase::amount));
     * }</pre>
     *
     * @param getDoubleValueFn function that extracts the {@code double} values
     *     you want to sum. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, DoubleAccumulator, Double> summingDouble(
            @Nonnull ToDoubleFunctionEx<? super T> getDoubleValueFn
    ) {
        checkSerializable(getDoubleValueFn, "getDoubleValueFn");
        return AggregateOperation
                .withCreate(DoubleAccumulator::new)
                .andAccumulate((DoubleAccumulator a, T item) -> a.accumulate(getDoubleValueFn.applyAsDouble(item)))
                .andCombine(DoubleAccumulator::combine)
                .andDeduct(DoubleAccumulator::deduct)
                .andExportFinish(DoubleAccumulator::export);
    }

    /**
     * Returns an aggregate operation that computes the least item according to
     * the given {@code comparator}.
     * <p>
     * This sample takes a stream of people and finds the youngest person in it:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Person> youngestPerson =
     *         people.aggregate(minBy(ComparatorEx.comparing(Person::age)));
     * }</pre>
     * <strong>NOTE:</strong> if this aggregate operation doesn't observe any
     * items, its result will be {@code null}. Since the non-keyed {@link
     * BatchStage#aggregate} emits just the naked aggregation result, and since
     * a {@code null} cannot travel through a Jet pipeline, you will not get
     * any output in that case.
     * <p>
     * If several items tie for the least one, this aggregate operation will
     * choose any one to return and may choose a different one each time.
     * <p>
     * <em>Implementation note:</em> this aggregate operation does not
     * implement the {@link AggregateOperation1#deductFn() deduct} primitive.
     * This has performance implications for <a
     * href="https://jet-start.sh/docs/architecture/sliding-window">sliding
     * window aggregation</a>.
     *
     * @param comparator comparator to compare the items. It must be stateless
     *     and {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, MutableReference<T>, T> minBy(
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        return maxBy(comparator.reversed());
    }

    /**
     * Returns an aggregate operation that computes the greatest item according
     * to the given {@code comparator}.
     * <p>
     * This sample takes a stream of people and finds the oldest person in it:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Person> oldestPerson =
     *         people.aggregate(maxBy(ComparatorEx.comparing(Person::age)));
     * }</pre>
     * <strong>NOTE:</strong> if this aggregate operation doesn't observe any
     * items, its result will be {@code null}. Since the non-keyed {@link
     * BatchStage#aggregate} emits just the naked aggregation result, and since
     * a {@code null} cannot travel through a Jet pipeline, you will not get
     * any output in that case.
     * <p>
     * If several items tie for the greatest one, this aggregate operation will
     * choose any one to return and may choose a different one each time.
     * <p>
     * <em>Implementation note:</em> this aggregate operation does not
     * implement the {@link AggregateOperation1#deductFn() deduct} primitive.
     * This has performance implications for <a
     * href="https://jet-start.sh/docs/architecture/sliding-window">sliding
     * window aggregation</a>.
     *
     * @param comparator comparator to compare the items. It must be stateless
     *     and {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, MutableReference<T>, T> maxBy(
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        return AggregateOperation
                .withCreate(MutableReference<T>::new)
                .andAccumulate((MutableReference<T> a, T i) -> {
                    if (a.isNull() || comparator.compare(i, a.get()) > 0) {
                        a.set(i);
                    }
                })
                .andCombine((a1, a2) -> {
                    if (a1.isNull() || comparator.compare(a1.get(), a2.get()) < 0) {
                        a1.set(a2.get());
                    }
                })
                .andExportFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation that finds the top {@code n} items
     * according to the given {@link ComparatorEx comparator}. It outputs a
     * sorted list with the top item in the first position.
     * <p>
     * This sample takes a stream of people and finds ten oldest persons in it:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<List<Person>> oldestDudes =
     *         people.aggregate(topN(10, ComparatorEx.comparing(Person::age)));
     * }</pre>
     * <em>Implementation note:</em> this aggregate operation does not
     * implement the {@link AggregateOperation1#deductFn() deduct} primitive.
     * This has performance implications for <a
     * href="https://jet-start.sh/docs/architecture/sliding-window">sliding
     * window aggregation</a>.
     *
     * @param n number of top items to find
     * @param comparator compares the items. It must be stateless and
     *     {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, PriorityQueue<T>, List<T>> topN(
            int n, @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        ComparatorEx<? super T> comparatorReversed = comparator.reversed();
        BiConsumerEx<PriorityQueue<T>, T> accumulateFn = (queue, item) -> {
            if (queue.size() == n) {
                if (comparator.compare(item, queue.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }
                queue.poll();
            }
            queue.offer(item);
        };
        return AggregateOperation
                .withCreate(() -> new PriorityQueue<T>(n, comparator))
                .andAccumulate(accumulateFn)
                .andCombine((left, right) -> {
                    for (T item : right) {
                        accumulateFn.accept(left, item);
                    }
                })
                .andExportFinish(queue -> {
                    ArrayList<T> res = new ArrayList<>(queue);
                    res.sort(comparatorReversed);
                    return res;
                });
    }

    /**
     * Returns an aggregate operation that finds the bottom {@code n} items
     * according to the given {@link ComparatorEx comparator}. It outputs a
     * sorted list with the bottom item in the first position.
     * <p>
     * This sample takes a stream of people and finds ten youngest persons in
     * it:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<List<Person>> youngestDudes =
     *         people.aggregate(bottomN(10, ComparatorEx.comparing(Person::age)));
     * }</pre>
     * <em>Implementation note:</em> this aggregate operation does not
     * implement the {@link AggregateOperation1#deductFn() deduct} primitive.
     * This has performance implications for <a
     * href="https://jet-start.sh/docs/architecture/sliding-window">sliding
     * window aggregation</a>.
     *
     * @param n number of bottom items to find
     * @param comparator compares the items. It must be stateless and
     *     {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, PriorityQueue<T>, List<T>> bottomN(
            int n, @Nonnull ComparatorEx<? super T> comparator
    ) {
        return topN(n, comparator.reversed());
    }

    /**
     * Returns an aggregate operation that finds the arithmetic mean (aka.
     * average) of the {@code long} values it obtains by applying {@code
     * getLongValueFn} to each item. It outputs the result as a {@code double}.
     * <p>
     * This sample takes a stream of people and finds their mean age:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Double> meanAge = people.aggregate(averagingLong(Person::age));
     * }</pre>
     * <p>
     * If the aggregate operation does not observe any input, its result is
     * {@link Double#NaN NaN}.
     * <p>
     * <strong>NOTE:</strong> this operation accumulates the sum and the
     * count as separate {@code long} variables and combines them at the end
     * into the mean value. If either of these variables exceeds {@code
     * Long.MAX_VALUE}, the job will fail with an {@link ArithmeticException}.
     *
     * @param getLongValueFn function that extracts the {@code long} value from
     *     the item. It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongLongAccumulator, Double> averagingLong(
            @Nonnull ToLongFunctionEx<? super T> getLongValueFn
    ) {
        checkSerializable(getLongValueFn, "getLongValueFn");
        // count == accumulator.value1
        // sum == accumulator.value2
        return AggregateOperation
                .withCreate(LongLongAccumulator::new)
                .andAccumulate((LongLongAccumulator a, T i) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.get1() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.set1(a.get1() + 1);
                    a.set2(Math.addExact(a.get2(), getLongValueFn.applyAsLong(i)));
                })
                .andCombine((a1, a2) -> {
                    a1.set1(Math.addExact(a1.get1(), a2.get1()));
                    a1.set2(Math.addExact(a1.get2(), a2.get2()));
                })
                .andDeduct((a1, a2) -> {
                    a1.set1(Math.subtractExact(a1.get1(), a2.get1()));
                    a1.set2(Math.subtractExact(a1.get2(), a2.get2()));
                })
                .andExportFinish(a -> (double) a.get2() / a.get1());
    }

    /**
     * Returns an aggregate operation that finds the arithmetic mean (aka.
     * average) of the {@code double} values it obtains by applying {@code
     * getDoubleValueFn} to each item. It outputs the result as a {@code double}.
     * <p>
     * This sample takes a stream of people and finds their mean age:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Double> meanAge = people.aggregate(averagingDouble(Person::age));
     * }</pre>
     * <p>
     * If the aggregate operation does not observe any input, its result is
     * {@link Double#NaN NaN}.
     *
     * @param getDoubleValueFn function that extracts the {@code double} value
     *     from the item. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LongDoubleAccumulator, Double> averagingDouble(
            @Nonnull ToDoubleFunctionEx<? super T> getDoubleValueFn
    ) {
        checkSerializable(getDoubleValueFn, "getDoubleValueFn");
        // count == accumulator.value1
        // sum == accumulator.value2
        return AggregateOperation
                .withCreate(LongDoubleAccumulator::new)
                .andAccumulate((LongDoubleAccumulator a, T item) -> {
                    // a bit faster check than in addExact, specialized for increment
                    if (a.getLong() == Long.MAX_VALUE) {
                        throw new ArithmeticException("Counter overflow");
                    }
                    a.setLong(a.getLong() + 1);
                    a.setDouble(a.getDouble() + getDoubleValueFn.applyAsDouble(item));
                })
                .andCombine((a1, a2) -> {
                    a1.setLong(Math.addExact(a1.getLong(), a2.getLong()));
                    a1.setDouble(a1.getDouble() + a2.getDouble());
                })
                .andDeduct((a1, a2) -> {
                    a1.setLong(Math.subtractExact(a1.getLong(), a2.getLong()));
                    a1.setDouble(a1.getDouble() - a2.getDouble());
                })
                .andExportFinish(a -> a.getDouble() / a.getLong());
    }

    /**
     * Returns an aggregate operation that computes a linear trend over the
     * items. It will produce a {@code double}-valued coefficient that
     * approximates the rate of change of {@code y} as a function of {@code x},
     * where {@code x} and {@code y} are {@code long} quantities obtained
     * by applying the two provided functions to each item.
     * <p>
     * This sample takes an infinite stream of trade events and outputs the
     * current rate of price change using a sliding window:
     * <pre>{@code
     * StreamStage<Trade> trades = pipeline
     *         .readFrom(tradeSource)
     *         .withTimestamps(Trade::getTimestamp, SECONDS.toMillis(1));
     * StreamStage<WindowResult<Double>> priceTrend = trades
     *     .window(WindowDefinition.sliding(MINUTES.toMillis(5), SECONDS.toMillis(1)))
     *     .aggregate(linearTrend(Trade::getTimestamp, Trade::getPrice));
     * }</pre>
     * With the trade price given in cents and the timestamp in milliseconds,
     * the output will be in cents per millisecond. Make sure you apply a
     * scaling factor if you want another, more natural unit of measure.
     * <p>
     * If this aggregate operation does not observe any input, its result is
     * {@link Double#NaN NaN}.
     *
     * @param getXFn a function to extract <strong>x</strong> from the input.
     *     It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @param getYFn a function to extract <strong>y</strong> from the input.
     *     It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @param <T> type of the input item
     */
    @Nonnull
    public static <T> AggregateOperation1<T, LinTrendAccumulator, Double> linearTrend(
            @Nonnull ToLongFunctionEx<T> getXFn,
            @Nonnull ToLongFunctionEx<T> getYFn
    ) {
        checkSerializable(getXFn, "getXFn");
        checkSerializable(getYFn, "getYFn");
        return AggregateOperation
                .withCreate(LinTrendAccumulator::new)
                .andAccumulate((LinTrendAccumulator a, T item) ->
                        a.accumulate(getXFn.applyAsLong(item), getYFn.applyAsLong(item)))
                .andCombine(LinTrendAccumulator::combine)
                .andDeduct(LinTrendAccumulator::deduct)
                .andExportFinish(LinTrendAccumulator::export);
    }

    /**
     * Returns an aggregate operation that takes string items and concatenates
     * them into a single string.
     * <p>
     * This sample outputs a string that you get by reading down the first
     * column of the input text:
     * <pre>{@code
     * BatchStage<String> linesOfText = pipeline.readFrom(textSource);
     * BatchStage<String> lineStarters = linesOfText
     *         .map(line -> line.charAt(0))
     *         .map(Object::toString)
     *         .aggregate(concatenating());
     * }</pre>
     */
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating() {
        return AggregateOperation
                .withCreate(StringBuilder::new)
                .<CharSequence>andAccumulate(StringBuilder::append)
                .andCombine(StringBuilder::append)
                .andExportFinish(StringBuilder::toString);
    }

    /**
     * Returns an aggregate operation that takes string items and concatenates
     * them, separated by the given {@code delimiter}, into a single string.
     * <p>
     * This sample outputs a single line of text that contains all the
     * upper-cased and title-cased words of the input text:
     * <pre>{@code
     * BatchStage<String> linesOfText = pipeline.readFrom(textSource);
     * BatchStage<String> upcaseWords = linesOfText
     *         .map(line -> line.split("\\W+"))
     *         .flatMap(Traversers::traverseArray)
     *         .filter(word -> word.matches("\\p{Lu}.*"))
     *         .aggregate(concatenating(" "));
     * }</pre>
     */
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating(
            CharSequence delimiter
    ) {
        return concatenating(delimiter, "", "");
    }

    /**
     * Returns an aggregate operation that takes string items and concatenates
     * them, separated by the given {@code delimiter}, into a single string.
     * The resulting string will start with the given {@code prefix} and end
     * with the given {@code suffix}.
     * <p>
     * This sample outputs a single item, a JSON array of all the upper-cased
     * and title-cased words of the input text:
     * <pre>{@code
     * BatchStage<String> linesOfText = pipeline.readFrom(textSource);
     * BatchStage<String> upcaseWords = linesOfText
     *         .map(line -> line.split("\\W+"))
     *         .flatMap(Traversers::traverseArray)
     *         .filter(word -> word.matches("\\p{Lu}.*"))
     *         .aggregate(concatenating("['", "', '", "']"));
     * }</pre>
     */
    public static AggregateOperation1<CharSequence, StringBuilder, String> concatenating(
            CharSequence delimiter, CharSequence prefix, CharSequence suffix
    ) {
        int prefixLen = prefix.length();
        return AggregateOperation
                .withCreate(() -> new StringBuilder().append(prefix))
                .<CharSequence>andAccumulate((builder, val) -> {
                    if (builder.length() != prefixLen && val.length() > 0) {
                        builder.append(delimiter);
                    }
                    builder.append(val);
                })
                .andCombine((l, r) -> {
                    if (l.length() != prefixLen && r.length() != prefixLen) {
                        l.append(delimiter);
                    }
                    l.append(r, prefixLen, r.length());
                })
                .andExportFinish(r -> {
                    try {
                        return r.append(suffix).toString();
                    } finally {
                        r.setLength(r.length() - suffix.length());
                    }
                });
    }

    /**
     * Adapts an aggregate operation that takes items of type {@code U} to one
     * that takes items of type {@code T}, by applying the given mapping
     * function to each item. Normally you should just apply the mapping in a
     * stage before the aggregation, but this adapter is useful when
     * simultaneously performing several aggregate operations using {@link
     * #allOf}.
     * <p>
     * In addition to mapping, you can apply filtering as well by returning
     * {@code null} for an item you want filtered out.
     * <p>
     * This sample takes a stream of people and builds two sorted lists from
     * it, one with all the names and one with all the surnames:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Tuple2<List<String>, List<String>>> sortedNames =
     *     people.aggregate(allOf(
     *         mapping(Person::getFirstName, sorting(ComparatorEx.naturalOrder())),
     *         mapping(Person::getLastName, sorting(ComparatorEx.naturalOrder()))));
     * }</pre>
     *
     * @see #filtering
     * @see #flatMapping
     *
     * @param mapFn the function to apply to the input items. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param downstream the downstream aggregate operation
     * @param <T> type of the input item
     * @param <U> input type of the downstream aggregate operation
     * @param <A> downstream operation's accumulator type
     * @param <R> downstream operation's result type
     */
    public static <T, U, A, R> AggregateOperation1<T, A, R> mapping(
            @Nonnull FunctionEx<? super T, ? extends U> mapFn,
            @Nonnull AggregateOperation1<? super U, A, ? extends R> downstream
    ) {
        checkSerializable(mapFn, "mapFn");
        BiConsumerEx<? super A, ? super U> downstreamAccumulateFn = downstream.accumulateFn();
        return AggregateOperation
                .withCreate(downstream.createFn())
                .andAccumulate((A a, T t) -> {
                    U mapped = mapFn.apply(t);
                    if (mapped != null) {
                        downstreamAccumulateFn.accept(a, mapped);
                    }
                })
                .andCombine(downstream.combineFn())
                .andDeduct(downstream.deductFn())
                .<R>andExport(downstream.exportFn())
                .andFinish(downstream.finishFn());
    }

    /**
     * Adapts an aggregate operation so that it accumulates only the items
     * passing the {@code filterFn} and ignores others. Normally you should
     * just apply the filter in a stage before the aggregation, but this
     * adapter is useful when simultaneously performing several aggregate
     * operations using {@link #allOf}.
     * <p>
     * This sample takes a stream of people and outputs two numbers, the
     * average height of kids and grown-ups:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Tuple2<Double, Double>> avgHeightByAge = people.aggregate(allOf(
     *     filtering((Person p) -> p.getAge() < 18, averagingLong(Person::getHeight)),
     *     filtering((Person p) -> p.getAge() >= 18, averagingLong(Person::getHeight))
     * ));
     * }</pre>
     * @see #mapping
     * @see #flatMapping
     *
     * @param filterFn the filtering function. It must be stateless and
     *     {@linkplain Processor#isCooperative() cooperative}.
     * @param downstream the downstream aggregate operation
     * @param <T> type of the input item
     * @param <A> downstream operation's accumulator type
     * @param <R> downstream operation's result type
     *
     * @since Jet 3.1
     */
    public static <T, A, R> AggregateOperation1<T, A, R> filtering(
            @Nonnull PredicateEx<? super T> filterFn,
            @Nonnull AggregateOperation1<? super T, A, ? extends R> downstream
    ) {
        checkSerializable(filterFn, "filterFn");
        BiConsumerEx<? super A, ? super T> downstreamAccumulateFn = downstream.accumulateFn();
        return AggregateOperation
                .withCreate(downstream.createFn())
                .andAccumulate((A a, T t) -> {
                    if (filterFn.test(t)) {
                        downstreamAccumulateFn.accept(a, t);
                    }
                })
                .andCombine(downstream.combineFn())
                .andDeduct(downstream.deductFn())
                .<R>andExport(downstream.exportFn())
                .andFinish(downstream.finishFn());
    }

    /**
     * Adapts an aggregate operation that takes items of type {@code U} to one
     * that takes items of type {@code T}, by exploding each {@code T} into a
     * sequence of {@code U}s and then accumulating all of them. Normally you
     * should just apply the flat-mapping in a stage before the aggregation,
     * but this adapter is useful when simultaneously performing several
     * aggregate operations using {@link #allOf}.
     * <p>
     * The traverser your function returns must be non-null and
     * <em>null-terminated</em>.
     * <p>
     * This sample takes a stream of people and outputs two numbers, the mean
     * age of all the people and the mean age of people listed as someone's
     * kid:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * people.aggregate(allOf(
     *     averagingLong(Person::getAge),
     *     flatMapping((Person p) -> traverseIterable(p.getChildren()),
     *             averagingLong(Person::getAge))
     * ));
     * }</pre>
     * @see #mapping
     * @see #filtering
     *
     * @param flatMapFn the flat-mapping function to apply. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param downstream the downstream aggregate operation
     * @param <T> type of the input item
     * @param <U> input type of the downstream aggregate operation
     * @param <A> downstream operation's accumulator type
     * @param <R> downstream operation's result type
     *
     * @since Jet 3.1
     */
    public static <T, U, A, R> AggregateOperation1<T, A, R> flatMapping(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends U>> flatMapFn,
            @Nonnull AggregateOperation1<? super U, A, ? extends R> downstream
    ) {
        checkSerializable(flatMapFn, "flatMapFn");
        BiConsumerEx<? super A, ? super U> downstreamAccumulateFn = downstream.accumulateFn();
        return AggregateOperation
                .withCreate(downstream.createFn())
                .andAccumulate((A a, T t) -> {
                    Traverser<? extends U> trav = flatMapFn.apply(t);
                    for (U u; (u = trav.next()) != null; ) {
                        downstreamAccumulateFn.accept(a, u);
                    }
                })
                .andCombine(downstream.combineFn())
                .andDeduct(downstream.deductFn())
                .<R>andExport(downstream.exportFn())
                .andFinish(downstream.finishFn());
    }

    /**
     * Returns an aggregate operation that accumulates the items into a {@code
     * Collection}. It creates empty, mutable collections as needed by calling
     * the provided {@code createCollectionFn}.
     * <p>
     * This sample takes a stream of words and outputs a single sorted set of
     * all the long words (above 5 letters):
     * <pre>{@code
     * BatchStage<String> words = pipeline.readFrom(wordSource);
     * BatchStage<SortedSet<String>> sortedLongWords = words
     *         .filter(w -> w.length() > 5)
     *         .aggregate(toCollection(TreeSet::new));
     * }</pre>
     * <strong>Note:</strong> if you use a collection that preserves the
     * insertion order, keep in mind that Jet doesn't aggregate the items in
     * any specified order.
     *
     * @param createCollectionFn a {@code Supplier} of empty, mutable {@code
     *     Collection}s. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     * @param <C> the type of the collection
     */
    public static <T, C extends Collection<T>> AggregateOperation1<T, C, C> toCollection(
            @Nonnull SupplierEx<C> createCollectionFn
    ) {
        checkSerializable(createCollectionFn, "createCollectionFn");
        return AggregateOperation
                .withCreate(createCollectionFn)
                .<T>andAccumulate(Collection::add)
                .andCombine(Collection::addAll)
                .andExport(acc -> {
                    C result = createCollectionFn.get();
                    result.addAll(acc);
                    return result;
                })
                .andFinish(identity());
    }

    /**
     * Returns an aggregate operation that accumulates the items into an {@code
     * ArrayList}.
     * <p>
     * This sample takes a stream of words and outputs a single list of all the
     * long words (above 5 letters):
     * <pre>{@code
     * BatchStage<String> words = pipeline.readFrom(wordSource);
     * BatchStage<List<String>> longWords = words
     *         .filter(w -> w.length() > 5)
     *         .aggregate(toList());
     * }</pre>
     * <strong>Note:</strong> accumulating all the data into an in-memory list
     * shouldn't be your first choice in designing a pipeline. Consider
     * draining the result stream to a sink.
     *
     * @param <T> type of the input item
     */
    public static <T> AggregateOperation1<T, List<T>, List<T>> toList() {
        return toCollection(ArrayList::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a {@code
     * HashSet}.
     * <p>
     * This sample takes a stream of people and outputs a single set of all the
     * distinct cities they live in:
     * <pre>{@code
     * pipeline.readFrom(personSource)
     *         .map(Person::getCity)
     *         .aggregate(toSet());
     * }</pre>
     * <strong>Note:</strong> accumulating all the data into an in-memory set
     * shouldn't be your first choice in designing a pipeline. Consider
     * draining the result stream to a sink.
     * @param <T> type of the input item
     */
    public static <T> AggregateOperation1<T, Set<T>, Set<T>> toSet() {
        return toCollection(HashSet::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a {@code
     * HashMap} whose keys and values are the result of applying the provided
     * mapping functions.
     * <p>
     * This aggregate operation does not tolerate duplicate keys and will throw
     * an {@code IllegalStateException} if it detects them. If your data
     * contains duplicates, use {@link #toMap(FunctionEx, FunctionEx,
     * BinaryOperatorEx) toMap(keyFn, valueFn, mergeFn)}.
     * <p>
     * The following sample takes a stream of sensor readings and outputs a
     * single map {sensor ID -> reading}:
     * <pre>{@code
     * BatchStage<Map<String, Double>> readings = pipeline
     *         .readFrom(sensorData)
     *         .aggregate(toMap(
     *                 SensorReading::getSensorId,
     *                 SensorReading::getValue));
     * }</pre>
     * <strong>Note:</strong> accumulating all the data into an in-memory map
     * shouldn't be your first choice in designing a pipeline. Consider
     * draining the stream to a sink.
     *
     * @param keyFn a function to extract the key from the input item. It must
     *     be stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param valueFn a function to extract the value from the input item. It
     *     must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @param <T> type of the input item
     * @param <K> type of the key
     * @param <U> type of the value
     *
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx, SupplierEx)
     * @see #groupingBy(FunctionEx)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        return toMap(keyFn, valueFn,
                (k, v) -> {
                    throw new IllegalStateException("Duplicate key: " + k);
                },
                HashMap::new);
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} whose keys and values are the result of applying
     * the provided mapping functions.
     * <p>
     * This aggregate operation resolves duplicate keys by applying {@code
     * mergeFn} to the conflicting values. {@code mergeFn} will act upon the
     * values after {@code valueFn} has already been applied.
     * <p>
     * The following sample takes a stream of sensor readings and outputs a
     * single map {sensor ID -> reading}. Multiple readings from the same
     * sensor get summed up:
     * <pre>{@code
     * BatchStage<Map<String, Double>> readings = pipeline
     *         .readFrom(sensorData)
     *         .aggregate(toMap(
     *                 SensorReading::getSensorId,
     *                 SensorReading::getValue,
     *                 Double::sum));
     * }</pre>
     * <strong>Note:</strong> accumulating all the data into an in-memory map
     * shouldn't be your first choice in designing a pipeline. Consider
     * draining the stream to a sink.
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param keyFn a function to extract the key from input item
     * @param valueFn a function to extract value from input item
     * @param mergeFn the function used to resolve collisions between values associated
     *                with the same key, will be passed to {@link Map#merge(Object, Object,
     *                java.util.function.BiFunction)}
     * @param <T> type of the input item
     * @param <K> the type of key
     * @param <U> the output type of the value mapping function
     *
     * @see #toMap(FunctionEx, FunctionEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx, SupplierEx)
     */
    public static <T, K, U> AggregateOperation1<T, Map<K, U>, Map<K, U>> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn,
            BinaryOperatorEx<U> mergeFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        return toMap(keyFn, valueFn, mergeFn, HashMap::new);
    }

    /**
     * Returns an aggregate operation that accumulates elements into a
     * user-supplied {@code Map} instance. The keys and values are the result
     * of applying the provided mapping functions to the input elements.
     * <p>
     * This aggregate operation resolves duplicate keys by applying {@code
     * mergeFn} to the conflicting values. {@code mergeFn} will act upon the
     * values after {@code valueFn} has already been applied.
     * <p>
     * The following sample takes a stream of sensor readings and outputs a
     * single {@code ObjectToLongHashMap} of {sensor ID -> reading}. Multiple
     * readings from the same sensor get summed up:
     * <pre>{@code
     * BatchStage<Map<String, Long>> readings = pipeline
     *         .readFrom(sensorData)
     *         .aggregate(toMap(
     *                 SensorReading::getSensorId,
     *                 SensorReading::getValue,
     *                 Long::sum,
     *                 ObjectToLongHashMap::new));
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param keyFn a function to extract the key from input item
     * @param valueFn a function to extract value from input item
     * @param mergeFn a merge function, used to resolve collisions between
     *                      values associated with the same key, as supplied
     *                      to {@link Map#merge(Object, Object,
     *                      java.util.function.BiFunction)}
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *                    which the results will be inserted
     * @param <T> type of the input item
     * @param <K> the output type of the key mapping function
     * @param <U> the output type of the value mapping function
     * @param <M> the type of the resulting {@code Map}
     *
     * @see #toMap(FunctionEx, FunctionEx)
     * @see #toMap(FunctionEx, FunctionEx, BinaryOperatorEx)
     */
    public static <T, K, U, M extends Map<K, U>> AggregateOperation1<T, M, M> toMap(
            FunctionEx<? super T, ? extends K> keyFn,
            FunctionEx<? super T, ? extends U> valueFn,
            BinaryOperatorEx<U> mergeFn,
            SupplierEx<M> createMapFn
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(valueFn, "valueFn");
        checkSerializable(mergeFn, "mergeFn");
        checkSerializable(createMapFn, "createMapFn");
        BiConsumerEx<M, T> accumulateFn =
                (map, element) -> map.merge(keyFn.apply(element), valueFn.apply(element), mergeFn);
        return AggregateOperation
                .withCreate(createMapFn)
                .andAccumulate(accumulateFn)
                .andCombine((l, r) -> r.forEach((key, value) -> l.merge(key, value, mergeFn)))
                .andExport(acc -> {
                    M result = createMapFn.get();
                    result.putAll(acc);
                    return result;
                })
                .andFinish(identity());
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code keyFn}
     * and the value is a list of the items with that key.
     * <p>
     * This operation is primarily useful when you need a cascaded group-by
     * where you further classify the members of each group by a secondary key.
     * <p>
     * This sample takes a stream of persons and classifies them first by
     * country and then by gender. It outputs a stream of map entries where the
     * key is the country and the value is a map from gender to the list of
     * people of that gender from that country:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(personSource);
     * BatchStage<Entry<String, Map<String, List<Person>>>> byCountryAndGender =
     *         people.groupingKey(Person::getCountry)
     *               .aggregate(groupingBy(Person::getGender));
     *     }
     * }</pre>
     *
     * This aggregate operation has a similar effect to the dedicated {@link
     * GeneralStage#groupingKey(FunctionEx) groupingKey()} pipeline transform
     * so you may wonder why not use it in all cases, not just cascaded
     * grouping. To see the difference, check out these two snippets:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(personSource);
     *
     * // Snippet 1
     * BatchStage<Entry<String, List<Person>>> byCountry1 =
     *         people.groupingKey(Person::getCountry)
     *               .aggregate(toList());
     *
     * // Snippet 2
     * BatchStage<Map<String, List<Person>>> byCountry2 =
     *         people.aggregate(groupingBy(Person::getCountry));
     * }</pre>
     *
     * Notice that snippet 1 outputs a <em>stream of map entries</em> whereas
     * snippet 2 outputs a <em>single map</em>. To produce the single map,
     * Jet must do all the work on a single thread and hold all the data on a
     * single cluster member, so you lose the advantage of distributed
     * computation. By contrast, snippet 1 allows Jet to partition the input by
     * the grouping key and split the work across the cluster. This is why you
     * should prefer a {@code groupingKey} stage if you have just one level of
     * grouping.
     *
     * @param keyFn a function to extract the key from input item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> type of the input item
     * @param <K> the output type of the key mapping function
     *
     * @see #groupingBy(FunctionEx, AggregateOperation1)
     * @see #groupingBy(FunctionEx, SupplierEx, AggregateOperation1)
     * @see #toMap(FunctionEx, FunctionEx)
     */
    public static <T, K> AggregateOperation1<T, Map<K, List<T>>, Map<K, List<T>>> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn
    ) {
        checkSerializable(keyFn, "keyFn");
        return groupingBy(keyFn, toList());
    }

    /**
     * Returns an aggregate operation that accumulates the items into a
     * {@code HashMap} where the key is the result of applying {@code keyFn}
     * and the value is the result of applying the downstream aggregate
     * operation to the items with that key.
     * <p>
     * This operation is primarily useful when you need a cascaded group-by
     * where you further classify the members of each group by a secondary key.
     * For the difference between this operation and the {@link
     * GeneralStage#groupingKey(FunctionEx) groupingKey()} pipeline transform,
     * see the documentation on {@link #groupingBy(FunctionEx) groupingBy(keyFn)}.
     * <p>
     * This sample takes a stream of people, classifies them by country and
     * gender, and reports the number of people in each category:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(personSource);
     * BatchStage<Entry<String, Map<String, Long>>> countByCountryAndGender =
     *         people.groupingKey(Person::getCountry)
     *               .aggregate(groupingBy(Person::getGender, counting()));
     * }</pre>
     *
     *
     * @param keyFn a function to extract the key from input item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param downstream the downstream aggregate operation
     * @param <T> type of the input item
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     *
     * @see #groupingBy(FunctionEx)
     * @see #groupingBy(FunctionEx, SupplierEx, AggregateOperation1)
     * @see #toMap(FunctionEx, FunctionEx)
     */
    public static <T, K, A, R> AggregateOperation1<T, Map<K, A>, Map<K, R>> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        checkSerializable(keyFn, "keyFn");
        return groupingBy(keyFn, HashMap::new, downstream);
    }

    /**
     * Returns an {@code AggregateOperation1} that accumulates the items into a
     * {@code Map} (as obtained from {@code createMapFn}) where the key is the
     * result of applying {@code keyFn} and the value is the result of
     * applying the downstream aggregate operation to the items with that key.
     * <p>
     * This operation is primarily useful when you need a cascaded group-by
     * where you further classify the members of each group by a secondary key.
     * For the difference between this operation and the {@link
     * GeneralStage#groupingKey(FunctionEx) groupingKey()} pipeline transform,
     * see the documentation on {@link #groupingBy(FunctionEx) groupingBy(keyFn)}.
     * <p>
     * The following sample takes a stream of people, classifies them by country
     * and gender, and reports the number of people in each category. It uses
     * the {@code EnumMap} to optimize memory usage:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(personSource);
     * BatchStage<Entry<String, Map<Gender, Long>>> countByCountryAndGender =
     *         people.groupingKey(Person::getCountry)
     *               .aggregate(groupingBy(
     *                       Person::getGender,
     *                       () -> new EnumMap<>(Gender.class),
     *                       counting()));
     * }</pre>
     *
     * @param keyFn a function to extract the key from input item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param createMapFn a function which returns a new, empty {@code Map} into
     *     which the results will be inserted. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param downstream the downstream aggregate operation
     * @param <T> type of the input item
     * @param <K> the output type of the key mapping function
     * @param <R> the type of the downstream aggregation result
     * @param <A> downstream aggregation's accumulator type
     * @param <M> output type of the resulting {@code Map}
     *
     * @see #groupingBy(FunctionEx)
     * @see #groupingBy(FunctionEx, AggregateOperation1)
     * @see #toMap(FunctionEx, FunctionEx)
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T, K, R, A, M extends Map<K, R>> AggregateOperation1<T, Map<K, A>, M> groupingBy(
            FunctionEx<? super T, ? extends K> keyFn,
            SupplierEx<M> createMapFn,
            AggregateOperation1<? super T, A, R> downstream
    ) {
        checkSerializable(keyFn, "keyFn");
        checkSerializable(createMapFn, "createMapFn");

        BiConsumerEx<? super Map<K, A>, T> accumulateFn = (m, t) -> {
            A acc = m.computeIfAbsent(keyFn.apply(t), k -> downstream.createFn().get());
            downstream.accumulateFn().accept(acc, t);
        };

        BiConsumerEx<Map<K, A>, Map<K, A>> combineFn;
        BiConsumerEx<? super A, ? super A> downstreamCombineFn = downstream.combineFn();
        if (downstreamCombineFn != null) {
            combineFn = (l, r) ->
                    r.forEach((key, value) -> l.merge(key, value, (a, b) -> {
                        downstreamCombineFn.accept(a, b);
                        return a;
                    }));
        } else {
            combineFn = null;
        }

        // replace the map contents with finished values
        SupplierEx<Map<K, A>> createAccMapFn = (SupplierEx<Map<K, A>>) createMapFn;

        return (AggregateOperation1<T, Map<K, A>, M>) AggregateOperation
                .withCreate(createAccMapFn)
                .andAccumulate(accumulateFn)
                .andCombine(combineFn)
                .andExport(accMap -> accMap.entrySet().stream()
                                           .collect(Collectors.toMap(
                                                   Entry::getKey,
                                                   e -> downstream.exportFn().apply(e.getValue()))))
                .andFinish(accMap -> {
                    // replace the values in map in-place
                    accMap.replaceAll((K k, A v) -> ((FunctionEx<A, A>) downstream.finishFn()).apply(v));
                    return (Map) accMap;
                });
    }

    /**
     * Returns an aggregate operation that constructs the result through the
     * process of <em>immutable reduction</em>:
     * <ol>
     *     <li>The initial accumulated value is {@code emptyAccValue}.
     *     <li>For each input item, compute the new value:
     *     {@code newVal = combineAccValues(currVal, toAccValue(item))}
     * </ol>
     * {@code combineAccValuesFn} must be <strong>associative</strong> because
     * it will also be used to combine partial results, as well as
     * <strong>commutative</strong> because the encounter order of items is
     * unspecified.
     * <p>
     * The optional {@code deductAccValueFn} allows Jet to compute the sliding
     * window in O(1) time. It must undo the effects of a previous {@code
     * combineAccValuesFn} call:
     * <pre>
     *     A accVal;  // has some pre-existing value
     *     A itemAccVal = toAccValueFn.apply(item);
     *     A combined = combineAccValuesFn.apply(accVal, itemAccVal);
     *     A deducted = deductAccValueFn.apply(combined, itemAccVal);
     *     assert deducted.equals(accVal);
     * </pre>
     * <p>
     * This sample takes a stream of product orders and outputs a single {@code
     * long} number which is the sum total of all the ordered amounts. The
     * aggregate operation it implements is equivalent to {@link #summingLong}:
     * <pre>{@code
     * BatchStage<Order> orders = pipeline.readFrom(orderSource);
     * BatchStage<Long> totalAmount = orders.aggregate(reducing(
     *         0L,
     *         Order::getAmount,
     *         Math::addExact,
     *         Math::subtractExact
     * ));
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param emptyAccValue the reducing operation's emptyAccValue element
     * @param toAccValueFn transforms the stream item into its accumulated value
     * @param combineAccValuesFn combines two accumulated values into one
     * @param deductAccValueFn deducts the right-hand accumulated value from the left-hand one
     *                        (optional)
     * @param <T> type of the input item
     * @param <A> type of the accumulated value
     */
    @Nonnull
    public static <T, A> AggregateOperation1<T, MutableReference<A>, A> reducing(
            @Nonnull A emptyAccValue,
            @Nonnull FunctionEx<? super T, ? extends A> toAccValueFn,
            @Nonnull BinaryOperatorEx<A> combineAccValuesFn,
            @Nullable BinaryOperatorEx<A> deductAccValueFn
    ) {
        checkSerializable(emptyAccValue, "emptyAccValue");
        checkSerializable(toAccValueFn, "toAccValueFn");
        checkSerializable(combineAccValuesFn, "combineAccValuesFn");
        checkSerializable(deductAccValueFn, "deductAccValueFn");

        @SuppressWarnings("UnnecessaryLocalVariable")
        BinaryOperatorEx<A> deductFn = deductAccValueFn;
        return AggregateOperation
                .withCreate(() -> new MutableReference<>(emptyAccValue))
                .andAccumulate((MutableReference<A> a, T t) ->
                        a.set(combineAccValuesFn.apply(a.get(), toAccValueFn.apply(t))))
                .andCombine((a, b) -> a.set(combineAccValuesFn.apply(a.get(), b.get())))
                .andDeduct(deductFn != null
                        ? (a, b) -> a.set(deductFn.apply(a.get(), b.get()))
                        : null)
                .andExportFinish(MutableReference::get);
    }

    /**
     * Returns an aggregate operation whose result is an arbitrary item it
     * observed, or {@code null} if it observed no items.
     * <p>
     * The implementation of {@link StageWithWindow#distinct()} uses this
     * operation and, if needed, you can use it directly for the same purpose.
     * <p>
     * This sample takes a stream of people and outputs a stream of people that
     * have distinct last names (same as calling {@link
     * BatchStageWithKey#distinct() groupingKey(keyFn).distinct()}:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<Entry<String, Person>> distinctByLastName =
     *         people.groupingKey(Person::getLastName)
     *               .aggregate(pickAny());
     * }</pre>
     * <strong>NOTE:</strong> if this aggregate operation doesn't observe any
     * items, its result will be {@code null}. Since the non-keyed {@link
     * BatchStage#aggregate} emits just the naked aggregation result, and since
     * a {@code null} cannot travel through a Jet pipeline, you will not get
     * any output in that case.
     * @param <T> type of the input item
     */
    @Nonnull
    @SuppressWarnings("checkstyle:needbraces")
    public static <T> AggregateOperation1<T, PickAnyAccumulator<T>, T> pickAny() {
        return AggregateOperation
                .withCreate(PickAnyAccumulator<T>::new)
                .<T>andAccumulate(PickAnyAccumulator::accumulate)
                .andCombine(PickAnyAccumulator::combine)
                .andDeduct(PickAnyAccumulator::deduct)
                .andExportFinish(PickAnyAccumulator::get);
    }

    /**
     * Returns an aggregate operation that accumulates all input items into an
     * {@code ArrayList} and sorts it with the given comparator. If you have
     * {@code Comparable} items that you want to sort in their natural order, use
     * {@link ComparatorEx#naturalOrder()}.
     * <p>
     * This sample takes a stream of people and outputs a single list of people
     * sorted by their last name:
     * <pre>{@code
     * BatchStage<Person> people = pipeline.readFrom(peopleSource);
     * BatchStage<List<Person>> sorted = people.aggregate(
     *     sorting(ComparatorEx.comparing(Person::getLastName)));
     * }</pre>
     *
     * @param comparator the comparator to use for sorting. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param <T> the type of input items
     */
    public static <T> AggregateOperation1<T, ArrayList<T>, List<T>> sorting(
            @Nonnull ComparatorEx<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        return AggregateOperation
                .withCreate(ArrayList<T>::new)
                .<T>andAccumulate(ArrayList::add)
                .andCombine(ArrayList::addAll)
                .andExport(list -> {
                    // sorting the accumulator doesn't harm - will make the next sort an easier job
                    list.sort(comparator);
                    return (List<T>) new ArrayList<>(list);
                })
                .andFinish(list -> {
                    list.sort(comparator);
                    return list;
                });
    }

    /**
     * Returns an aggregate operation that is a composite of two aggregate
     * operations. It allows you to calculate both aggregations over the same
     * items at once.
     * <p>
     * This sample takes a stream of orders and outputs a single tuple
     * containing the orders with the smallest and the largest amount:
     * <pre>{@code
     * BatchStage<Order> orders = pipeline.readFrom(orderSource);
     * BatchStage<Tuple2<Order, Order>> extremes = orders.aggregate(allOf(
     *         minBy(ComparatorEx.comparing(Order::getAmount)),
     *         maxBy(ComparatorEx.comparing(Order::getAmount)),
     *         Tuple2::tuple2
     * ));
     * }</pre>
     *
     * @param op0 1st operation
     * @param op1 2nd operation
     * @param exportFinishFn function combining the two results into a single
     *     target instance. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     *
     * @param <T> type of input items
     * @param <A0> 1st accumulator type
     * @param <A1> 2nd accumulator type
     * @param <R0> 1st result type
     * @param <R1> 2nd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A0, A1, R0, R1, R> AggregateOperation1<T, Tuple2<A0, A1>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull BiFunctionEx<? super R0, ? super R1, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple2(op0.createFn().get(), op1.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op0.accumulateFn().accept(acc.f0(), item);
                    op1.accumulateFn().accept(acc.f1(), item);
                })
                .andCombine(combine0 == null || combine1 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                        })
                .andDeduct(deduct0 == null || deduct1 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                        })
                .<R>andExport(acc ->
                        exportFinishFn.apply(op0.exportFn().apply(acc.f0()), op1.exportFn().apply(acc.f1())))
                .andFinish(acc ->
                        exportFinishFn.apply(op0.finishFn().apply(acc.f0()), op1.finishFn().apply(acc.f1())));
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * BiFunctionEx)} wrapping the two results in a {@link Tuple2}.
     */
    @Nonnull
    public static <T, A0, A1, R0, R1> AggregateOperation1<T, Tuple2<A0, A1>, Tuple2<R0, R1>> allOf(
            @Nonnull AggregateOperation1<? super T, A0, R0> op1,
            @Nonnull AggregateOperation1<? super T, A1, R1> op2
    ) {
        return allOf(op1, op2, Tuple2::tuple2);
    }

    /**
     * Returns an aggregate operation that is a composite of three aggregate
     * operations. It allows you to calculate all three over the same items
     * at once.
     * <p>
     * This sample takes a stream of orders and outputs a single tuple
     * containing the average amount ordered and the orders with the smallest
     * and the largest amount:
     * <pre>{@code
     * BatchStage<Order> orders = pipeline.readFrom(orderSource);
     * BatchStage<Tuple3<Double, Order, Order>> averageAndExtremes =
     *     orders.aggregate(allOf(
     *         averagingLong(Order::getAmount),
     *         minBy(ComparatorEx.comparing(Order::getAmount)),
     *         maxBy(ComparatorEx.comparing(Order::getAmount)),
     *         Tuple3::tuple3
     * ));
     * }</pre>
     *
     * @param op0 1st operation
     * @param op1 2nd operation
     * @param op2 3rd operation
     * @param exportFinishFn function combining the three results into a single
     *     target instance. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     *
     * @param <T> type of input items
     * @param <A0> 1st accumulator type
     * @param <A1> 2nd accumulator type
     * @param <A2> 3rd accumulator type
     * @param <R0> 1st result type
     * @param <R1> 2nd result type
     * @param <R2> 3rd result type
     * @param <R> final result type
     *
     * @return the composite operation
     */
    @Nonnull
    public static <T, A0, A1, A2, R0, R1, R2, R> AggregateOperation1<T, Tuple3<A0, A1, A2>, R> allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, ? extends R2> op2,
            @Nonnull TriFunction<? super R0, ? super R1, ? super R2, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A2, ? super A2> combine2 = op2.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        BiConsumerEx<? super A2, ? super A2> deduct2 = op2.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple3(op0.createFn().get(), op1.createFn().get(), op2.createFn().get()))
                .<T>andAccumulate((acc, item) -> {
                    op0.accumulateFn().accept(acc.f0(), item);
                    op1.accumulateFn().accept(acc.f1(), item);
                    op2.accumulateFn().accept(acc.f2(), item);
                })
                .andCombine(combine0 == null || combine1 == null || combine2 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                            combine2.accept(acc1.f2(), acc2.f2());
                        })
                .andDeduct(deduct0 == null || deduct1 == null || deduct2 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                            deduct2.accept(acc1.f2(), acc2.f2());
                        })
                .<R>andExport(acc -> exportFinishFn.apply(
                        op0.exportFn().apply(acc.f0()),
                        op1.exportFn().apply(acc.f1()),
                        op2.exportFn().apply(acc.f2())))
                .andFinish(acc -> exportFinishFn.apply(
                        op0.finishFn().apply(acc.f0()),
                        op1.finishFn().apply(acc.f1()),
                        op2.finishFn().apply(acc.f2())));
    }

    /**
     * Convenience for {@link #allOf(AggregateOperation1, AggregateOperation1,
     * AggregateOperation1, TriFunction)} wrapping the three results in a
     * {@link Tuple3}.
     */
    @Nonnull
    public static <T, A0, A1, A2, R0, R1, R2>
    AggregateOperation1<T, Tuple3<A0, A1, A2>, Tuple3<R0, R1, R2>>
    allOf(
            @Nonnull AggregateOperation1<? super T, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T, A2, ? extends R2> op2
    ) {
        return allOf(op0, op1, op2, Tuple3::tuple3);
    }

    /**
     * Returns a builder object that helps you create a composite of multiple
     * aggregate operations. The resulting aggregate operation will perform all
     * of the constituent operations at the same time and you can retrieve
     * individual results from the {@link ItemsByTag} object you'll get in the
     * output.
     * <p>
     * The builder object is primarily intended to build a composite of four or
     * more aggregate operations. For up to three operations, prefer the simpler
     * and more type-safe variants {@link #allOf(AggregateOperation1,
     * AggregateOperation1) allOf(op1, op2)} and {@link
     * #allOf(AggregateOperation1, AggregateOperation1, AggregateOperation1)
     * allOf(op1, op2, op3)}.
     * <p>
     * In the following sample we'll construct a composite aggregate operation
     * that takes a stream of orders and finds the extremes in terms of ordered
     * amount. Here's the input stage:
     * <pre>{@code
     * BatchStage<Order> orders = pipeline.readFrom(orderSource);
     * }</pre>
     *
     * Now we construct the aggregate operation using the builder:
     *
     * <pre>{@code
     * AllOfAggregationBuilder<Order> builder = allOfBuilder();
     * Tag<Order> minTag = builder.add(minBy(ComparatorEx.comparing(Order::getAmount)));
     * Tag<Order> maxTag = builder.add(maxBy(ComparatorEx.comparing(Order::getAmount)));
     * AggregateOperation1<Order, ?, ItemsByTag> aggrOp = builder.build();
     * }</pre>
     *
     * Finally, we apply the aggregate operation and use the tags we got
     * above to extract the components:
     *
     * <pre>{@code
     * BatchStage<ItemsByTag> extremes = orders.aggregate(aggrOp);
     * BatchStage<Tuple2<Order, Order>> extremesAsTuple =
     *         extremes.map(ibt -> tuple2(ibt.get(minTag), ibt.get(maxTag)));
     * }</pre>
     *
     * @param <T> type of input items
     */
    @Nonnull
    public static <T> AllOfAggregationBuilder<T> allOfBuilder() {
        return new AllOfAggregationBuilder<>();
    }

    /**
     * Returns an aggregate operation that is a composite of two independent
     * aggregate operations, each one accepting its own input. You need this
     * kind of operation in a two-way co-aggregating pipeline stage such as
     * {@link BatchStage#aggregate2(BatchStage, AggregateOperation2)}
     * stage0.aggregate2(stage1, compositeAggrOp)}. Before using this method,
     * see if you can instead use {@link
     * BatchStage#aggregate2(AggregateOperation1, BatchStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it's simpler and
     * doesn't require you to pre-compose the aggregate operations.
     * <p>
     * This method is suitable when you can express your computation as two
     * independent aggregate operations where you combine their final results.
     * If you need an operation that combines the two inputs in the
     * accumulation phase, you can create an aggregate operation by specifying
     * each primitive using the {@linkplain AggregateOperation#withCreate
     * aggregate operation builder}.
     * <p>
     * As a quick example, let's say you have two data streams coming from an
     * online store, consisting of user actions: page visits and payments:
     * <pre>{@code
     * BatchStage<PageVisit> pageVisits = pipeline.readFrom(pageVisitSource);
     * BatchStage<Payment> payments = pipeline.readFrom(paymentSource);
     * }</pre>
     * We want to find out how many page clicks each user did before buying a
     * product. We can do it like this:
     * <pre>{@code
     * BatchStage<Entry<Long, Double>> visitsPerPurchase = pageVisits
     *     .groupingKey(PageVisit::userId)
     *     .aggregate2(
     *         payments.groupingKey(Payment::userId),
     *         aggregateOperation2(counting(), counting(),
     *             (numPageVisits, numPayments) -> 1.0 * numPageVisits / numPayments
     *         ));
     * }</pre>
     * The output stage's stream contains a {@code Map.Entry} where the key is
     * the user ID and the value is the ratio of page visits to payments for
     * that user.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param exportFinishFn the function that transforms the individual aggregate results into the
     *                 overall result that the co-aggregating stage emits. It must be stateless
     *                 and {@linkplain Processor#isCooperative() cooperative}.
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <R> type of the result
     */
    public static <T0, A0, R0, T1, A1, R1, R> AggregateOperation2<T0, T1, Tuple2<A0, A1>, R> aggregateOperation2(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull BiFunctionEx<? super R0, ? super R1, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple2(op0.createFn().get(), op1.createFn().get()))
                .<T0>andAccumulate0((acc, item) -> op0.accumulateFn().accept(acc.f0(), item))
                .<T1>andAccumulate1((acc, item) -> op1.accumulateFn().accept(acc.f1(), item))
                .andCombine(combine0 == null || combine1 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                        })
                .andDeduct(deduct0 == null || deduct1 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                        })
                .<R>andExport(acc ->
                        exportFinishFn.apply(op0.exportFn().apply(acc.f0()), op1.exportFn().apply(acc.f1())))
                .andFinish(acc ->
                        exportFinishFn.apply(op0.finishFn().apply(acc.f0()), op1.finishFn().apply(acc.f1())));
    }

    /**
     * Convenience for {@link #aggregateOperation2(AggregateOperation1,
     *      AggregateOperation1, BiFunctionEx)
     * aggregateOperation2(aggrOp0, aggrOp1, finishFn)} that outputs a
     * {@code Tuple2(result0, result1)}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     */
    public static <T0, T1, A0, A1, R0, R1>
    AggregateOperation2<T0, T1, Tuple2<A0, A1>, Tuple2<R0, R1>>
    aggregateOperation2(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1
    ) {
        return aggregateOperation2(op0, op1, Tuple2::tuple2);
    }

    /**
     * Returns an aggregate operation that is a composite of three independent
     * aggregate operations, each one accepting its own input. You need this
     * kind of operation in the three-way co-aggregating pipeline stage:
     * {@link BatchStage#aggregate3(BatchStage, BatchStage, AggregateOperation3)}
     * stage0.aggregate3(stage1, stage2, compositeAggrOp)}. Before using this
     * method, see if you can instead use {@link
     * BatchStage#aggregate3(AggregateOperation1, BatchStage, AggregateOperation1,
     * BatchStage, AggregateOperation1) stage0.aggregate3(aggrOp0, stage1,
     * aggrOp1, stage2, aggrOp2)} because it's simpler and doesn't require you
     * to pre-compose the aggregate operations.
     * <p>
     * This method is suitable when you can express your computation as three
     * independent aggregate operations where you combine their final results.
     * If you need an operation that combines the inputs in the accumulation
     * phase, you can create an aggregate operation by specifying each
     * primitive using the {@linkplain AggregateOperation#withCreate aggregate
     * operation builder}.
     * <p>
     * As a quick example, let's say you have three data streams coming from an
     * online store, consisting of user actions: page visits, add-to-cart
     * actions and payments:
     * <pre>{@code
     * BatchStage<PageVisit> pageVisits = pipeline.readFrom(pageVisitSource);
     * BatchStage<AddToCart> addToCarts = pipeline.readFrom(addToCartSource);
     * BatchStage<Payment> payments = pipeline.readFrom(paymentSource);
     * }</pre>
     * We want to get these metrics per each user: how many page clicks they
     * did before buying a product, and how many products they bought per
     * purchase. We could do it like this:
     * <pre>{@code
     * BatchStage<Entry<Integer, Tuple2<Double, Double>>> userStats = pageVisits
     *     .groupingKey(PageVisit::userId)
     *     .aggregate3(
     *         addToCarts.groupingKey(AddToCart::userId),
     *         payments.groupingKey(Payment::userId),
     *         aggregateOperation3(counting(), counting(), counting(),
     *             (numPageVisits, numAddToCarts, numPayments) ->
     *                 tuple2(1.0 * numPageVisits / numPayments,
     *                        1.0 * numAddToCarts / numPayments
     *                 )
     *         ));
     * }</pre>
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param op2 the aggregate operation that will receive the third stage's input
     * @param exportFinishFn the function that transforms the individual aggregate results into the
     *                 overall result that the co-aggregating stage emits. It must be stateless
     *                 and {@linkplain Processor#isCooperative() cooperative}.
     *
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <T2> type of items in the third stage
     * @param <A2> type of the third aggregate operation's accumulator
     * @param <R2> type of the third aggregate operation's result
     * @param <R> type of the result
     */
    public static <T0, T1, T2, A0, A1, A2, R0, R1, R2, R>
    AggregateOperation3<T0, T1, T2, Tuple3<A0, A1, A2>, R> aggregateOperation3(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T2, A2, ? extends R2> op2,
            @Nonnull TriFunction<? super R0, ? super R1, ? super R2, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        BiConsumerEx<? super A0, ? super A0> combine0 = op0.combineFn();
        BiConsumerEx<? super A1, ? super A1> combine1 = op1.combineFn();
        BiConsumerEx<? super A2, ? super A2> combine2 = op2.combineFn();
        BiConsumerEx<? super A0, ? super A0> deduct0 = op0.deductFn();
        BiConsumerEx<? super A1, ? super A1> deduct1 = op1.deductFn();
        BiConsumerEx<? super A2, ? super A2> deduct2 = op2.deductFn();
        return AggregateOperation
                .withCreate(() -> tuple3(op0.createFn().get(), op1.createFn().get(), op2.createFn().get()))
                .<T0>andAccumulate0((acc, item) -> op0.accumulateFn().accept(acc.f0(), item))
                .<T1>andAccumulate1((acc, item) -> op1.accumulateFn().accept(acc.f1(), item))
                .<T2>andAccumulate2((acc, item) -> op2.accumulateFn().accept(acc.f2(), item))
                .andCombine(combine0 == null || combine1 == null || combine2 == null ? null :
                        (acc1, acc2) -> {
                            combine0.accept(acc1.f0(), acc2.f0());
                            combine1.accept(acc1.f1(), acc2.f1());
                            combine2.accept(acc1.f2(), acc2.f2());
                        })
                .andDeduct(deduct0 == null || deduct1 == null || deduct2 == null ? null :
                        (acc1, acc2) -> {
                            deduct0.accept(acc1.f0(), acc2.f0());
                            deduct1.accept(acc1.f1(), acc2.f1());
                            deduct2.accept(acc1.f2(), acc2.f2());
                        })
                .<R>andExport(acc -> exportFinishFn.apply(
                        op0.exportFn().apply(acc.f0()),
                        op1.exportFn().apply(acc.f1()),
                        op2.exportFn().apply(acc.f2())))
                .andFinish(acc -> exportFinishFn.apply(
                        op0.finishFn().apply(acc.f0()),
                        op1.finishFn().apply(acc.f1()),
                        op2.finishFn().apply(acc.f2())));
    }

    /**
     * Convenience for {@link #aggregateOperation3(AggregateOperation1, AggregateOperation1,
     *      AggregateOperation1, TriFunction)
     * aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, finishFn)} that outputs a
     * {@code Tuple3(result0, result1, result2)}.
     *
     * @param op0 the aggregate operation that will receive the first stage's input
     * @param op1 the aggregate operation that will receive the second stage's input
     * @param op2 the aggregate operation that will receive the third stage's input
     * @param <T0> type of items in the first stage
     * @param <A0> type of the first aggregate operation's accumulator
     * @param <R0> type of the first aggregate operation's result
     * @param <T1> type of items in the second stage
     * @param <A1> type of the second aggregate operation's accumulator
     * @param <R1> type of the second aggregate operation's result
     * @param <T2> type of items in the third stage
     * @param <A2> type of the third aggregate operation's accumulator
     * @param <R2> type of the third aggregate operation's result
     */
    public static <T0, T1, T2, A0, A1, A2, R0, R1, R2>
    AggregateOperation3<T0, T1, T2, Tuple3<A0, A1, A2>, Tuple3<R0, R1, R2>>
    aggregateOperation3(
            @Nonnull AggregateOperation1<? super T0, A0, ? extends R0> op0,
            @Nonnull AggregateOperation1<? super T1, A1, ? extends R1> op1,
            @Nonnull AggregateOperation1<? super T2, A2, ? extends R2> op2
    ) {
        return aggregateOperation3(op0, op1, op2, Tuple3::tuple3);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to create
     * an aggregate operation that accepts multiple inputs. You must supply
     * this kind of operation to a co-aggregating pipeline stage. You need this
     * builder if you're using the {@link BatchStage#aggregateBuilder()
     * stage.aggregateBuilder()}. Before deciding to use it, consider using
     * {@link BatchStage#aggregateBuilder(AggregateOperation1)
     * stage.aggregateBuilder(aggrOp0)} because it will allow you to directly
     * pass the aggregate operation for each joined stage, without requiring
     * you to build a composite operation through this builder. Finally, if
     * you're co-aggregating two or three streams, prefer the simpler and more
     * type-safe variants: {@link #aggregateOperation2} and {@link
     * #aggregateOperation3}.
     * <p>
     * This builder is suitable when you can express your computation as
     * independent aggregate operations on each input where you combine only
     * their final results. If you need an operation that combines the inputs
     * in the accumulation phase, you can create an aggregate operation by
     * specifying each primitive using the {@linkplain AggregateOperation#withCreate
     * aggregate operation builder}.
     * <p>
     * As a quick example, let's say you have two data streams coming from an
     * online store, consisting of user actions: page visits and payments:
     * <pre>{@code
     * BatchStage<PageVisit> pageVisits = pipeline.readFrom(pageVisitSource);
     * BatchStage<Payment> payments = pipeline.readFrom(paymentSource);
     * }</pre>
     * We want to find out how many page clicks each user did before buying a
     * product, and we want to do it using {@link BatchStage#aggregateBuilder()
     * stage.aggregateBuilder()}. Note that there will be two builders at play:
     * the <em>stage builder</em>, which joins the pipeline stages, and the
     * <em>aggregate operation builder</em> (obtained from this method). First
     * we obtain the stage builder and add our pipeline stages:
     *
     * <pre>{@code
     * GroupAggregateBuilder1<PageVisit, Long> stageBuilder =
     *         pageVisits.groupingKey(PageVisit::userId).aggregateBuilder();
     * Tag<PageVisit> visitTag_in = stageBuilder.tag0();
     * Tag<Payment> payTag_in = stageBuilder.add(payments.groupingKey(Payment::userId));
     * }</pre>
     *
     * Now we have the tags we need to build the aggregate operation, and while
     * building it we get new tags to get the results of the operation:
     *
     * <pre>{@code
     * CoAggregateOperationBuilder opBuilder = coAggregateOperationBuilder();
     * Tag<Long> visitTag = opBuilder.add(visitTag_in, counting());
     * Tag<Long> payTag = opBuilder.add(payTag_in, counting());
     * }</pre>
     *
     * We use these tags in the {@code exportFinishFn} we specify at the end:
     *
     * <pre>{@code
     * AggregateOperation<Object[], Double> aggrOp =
     *         opBuilder.build(ibt -> 1.0 * ibt.get(visitTag) / ibt.get(payTag));
     * }</pre>
     *
     * And now we're ready to construct the output stage:
     *
     * <pre>{@code
     * BatchStage<Entry<Long, Double>> visitsPerPurchase = stageBuilder.build(aggrOp);
     * }</pre>
     *
     * The output stage's stream contains {@code Map.Entry}s where the key is
     * the user ID and the value is the ratio of page visits to payments for
     * that user.
     */
    @Nonnull
    public static CoAggregateOperationBuilder coAggregateOperationBuilder() {
        return new CoAggregateOperationBuilder();
    }

    /**
     * Adapts this aggregate operation to a collector which can be passed to
     * {@link java.util.stream.Stream#collect(Collector)}.
     * <p>
     * This can be useful when you want to combine java.util.stream with Jet
     * aggregations. For example, the below can be used to do multiple aggregations
     * in a single pass over the same data set:
     * <pre>{@code
     *   Stream<Person> personStream = people.stream();
     *   personStream.collect(
     *     AggregateOperations.toCollector(
     *       AggregateOperations.allOf(
     *         AggregateOperations.counting(),
     *         AggregateOperations.averagingLong(p -> p.getAge())
     *       )
     *     )
     *   );
     * }</pre>
     */
    @Nonnull
    public static <T, A, R> Collector<T, A, R> toCollector(AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        BiConsumerEx<? super A, ? super A> combineFn = aggrOp.combineFn();
        if (combineFn == null) {
            throw new IllegalArgumentException("This aggregate operation doesn't implement combineFn()");
        }
        return Collector.of(
            aggrOp.createFn(),
            (acc, t) -> aggrOp.accumulateFn().accept(acc, t),
            (l, r) -> {
                combineFn.accept(l, r);
                return l;
            },
            a -> aggrOp.finishFn().apply(a));
    }

    /**
     * Adapts this aggregate operation to be used for {@link IMap#aggregate(Aggregator)}
     * calls.
     * <p>
     * Using {@code IMap} aggregations can be desirable when you want to make
     * use of {@linkplain IMap#addIndex indices} when doing aggregations and
     * want to use the Jet aggregations API instead of writing a custom
     * {@link Aggregator}.
     * <p>
     * For example, the following aggregation can be used to group people by
     * their age and find the counts for each group.
     * <pre>{@code
     *   IMap<Integer, Person> map = jet.getMap("people");
     *   Map<Integer, Long> counts = map.aggregate(
     *     AggregateOperations.toAggregator(
     *       AggregateOperations.groupingBy(
     *         e -> e.getValue().getGender(), AggregateOperations.counting()
     *       )
     *     )
     *   );
     * }
     * </pre>
     */
    @Nonnull
    public static <T, A, R> Aggregator<T, R> toAggregator(
        AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return new AggregateOpAggregator<>(aggrOp);
    }
}
