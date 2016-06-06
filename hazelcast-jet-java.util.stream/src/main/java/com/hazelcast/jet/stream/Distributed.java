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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.StreamUtil;
import com.hazelcast.jet.stream.impl.collectors.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.jet.stream.impl.StreamUtil.setPrivateField;

public abstract class Distributed {

    @FunctionalInterface
    public interface BiConsumer<T, U> extends java.util.function.BiConsumer<T, U>, Serializable {
    }

    @FunctionalInterface
    public interface BiFunction<T, U, R> extends java.util.function.BiFunction<T, U, R>, Serializable {
    }

    @FunctionalInterface
    public interface BinaryOperator<T> extends java.util.function.BinaryOperator<T>, Serializable {

        static <T> BinaryOperator<T> minBy(java.util.Comparator<? super T> comparator) {
            Objects.requireNonNull(comparator);
            return (a, b) -> comparator.compare(a, b) <= 0 ? a : b;
        }

        static <T> BinaryOperator<T> maxBy(java.util.Comparator<? super T> comparator) {
            Objects.requireNonNull(comparator);
            return (a, b) -> comparator.compare(a, b) >= 0 ? a : b;
        }
    }

    @FunctionalInterface
    public interface IntBinaryOperator extends java.util.function.IntBinaryOperator, Serializable {
    }

    @FunctionalInterface
    public interface DoubleBinaryOperator extends java.util.function.DoubleBinaryOperator, Serializable {
    }

    @FunctionalInterface
    public interface LongBinaryOperator extends java.util.function.LongBinaryOperator, Serializable {
    }

    @FunctionalInterface
    public interface BiPredicate<T, U> extends java.util.function.BiPredicate<T, U>, Serializable {
    }

    @FunctionalInterface
    public interface BooleanSupplier extends java.util.function.BooleanSupplier, Serializable {
    }

    @FunctionalInterface
    public interface Consumer<T> extends java.util.function.Consumer<T>, Serializable {
    }

    @FunctionalInterface
    public interface IntConsumer extends java.util.function.IntConsumer, Serializable {
    }

    @FunctionalInterface
    public interface DoubleConsumer extends java.util.function.DoubleConsumer, Serializable {
    }

    @FunctionalInterface
    public interface LongConsumer extends java.util.function.LongConsumer, Serializable {
    }

    @FunctionalInterface
    public interface ObjIntConsumer<T> extends java.util.function.ObjIntConsumer<T>, Serializable {
    }

    @FunctionalInterface
    public interface ObjLongConsumer<T> extends java.util.function.ObjLongConsumer<T>, Serializable {
    }

    @FunctionalInterface
    public interface ObjDoubleConsumer<T> extends java.util.function.ObjDoubleConsumer<T>, Serializable {
    }

    @FunctionalInterface
    public interface DoubleFunction<R> extends java.util.function.DoubleFunction<R>, Serializable {
    }

    @FunctionalInterface
    public interface DoubleToIntFunction extends java.util.function.DoubleToIntFunction, Serializable {
    }

    @FunctionalInterface
    public interface DoubleToLongFunction extends java.util.function.DoubleToLongFunction, Serializable {
    }

    @FunctionalInterface
    public interface Function<T, R> extends java.util.function.Function<T, R>, Serializable {
        default <V> Distributed.Function<T, V> andThen(Distributed.Function<? super R, ? extends V> after) {
            Objects.requireNonNull(after);
            return (T t) -> after.apply(apply(t));
        }

        /**
         * Returns a function that always returns its input argument.
         *
         * @param <T> the type of the input and output objects to the function
         * @return a function that always returns its input argument
         */
        static <T> Function<T, T> identity() {
            return t -> t;
        }
    }

    @FunctionalInterface
    public interface IntFunction<R> extends java.util.function.IntFunction<R>, Serializable {
    }

    @FunctionalInterface
    public interface IntToDoubleFunction extends java.util.function.IntToDoubleFunction, Serializable {
    }

    @FunctionalInterface
    public interface IntToLongFunction extends java.util.function.IntToLongFunction, Serializable {
    }

    @FunctionalInterface
    public interface LongFunction<R> extends java.util.function.LongFunction<R>, Serializable {
    }

    @FunctionalInterface
    public interface LongToDoubleFunction extends java.util.function.LongToDoubleFunction, Serializable {
    }

    @FunctionalInterface
    public interface LongToIntFunction extends java.util.function.LongToIntFunction, Serializable {
    }

    @FunctionalInterface
    public interface Predicate<T> extends java.util.function.Predicate<T>, Serializable {

        default Predicate<T> negate() {
            return (t) -> !test(t);
        }

        default Predicate<T> and(Predicate<? super T> other) {
            Objects.requireNonNull(other);
            return (t) -> test(t) && other.test(t);
        }

        default Predicate<T> or(Predicate<? super T> other) {
            Objects.requireNonNull(other);
            return (t) -> test(t) || other.test(t);
        }
    }

    @FunctionalInterface
    public interface IntPredicate extends java.util.function.IntPredicate, Serializable {
        default IntPredicate negate() {
            return (value) -> !test(value);
        }

        default IntPredicate or(IntPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }

        default IntPredicate and(IntPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }
    }

    @FunctionalInterface
    public interface DoublePredicate extends java.util.function.DoublePredicate, Serializable {
        default DoublePredicate negate() {
            return (value) -> !test(value);
        }

        default DoublePredicate or(DoublePredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }

        default DoublePredicate and(DoublePredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }
    }

    @FunctionalInterface
    public interface LongPredicate extends java.util.function.LongPredicate, Serializable {
        default LongPredicate negate() {
            return (value) -> !test(value);
        }

        default LongPredicate or(LongPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) || other.test(value);
        }

        default LongPredicate and(LongPredicate other) {
            Objects.requireNonNull(other);
            return (value) -> test(value) && other.test(value);
        }
    }

    @FunctionalInterface
    public interface Supplier<T> extends java.util.function.Supplier<T>, Serializable {
    }

    @FunctionalInterface
    public interface ToDoubleBiFunction<T, U> extends java.util.function.ToDoubleBiFunction<T, U>, Serializable {
    }

    @FunctionalInterface
    public interface ToDoubleFunction<T> extends java.util.function.ToDoubleFunction<T>, Serializable {
    }

    @FunctionalInterface
    public interface ToIntBiFunction<T, U> extends java.util.function.ToIntBiFunction<T, U>, Serializable {
    }

    @FunctionalInterface
    public interface ToLongBiFunction<T, U> extends java.util.function.ToLongBiFunction<T, U>, Serializable {
    }

    @FunctionalInterface
    public interface ToIntFunction<T> extends java.util.function.ToIntFunction<T>, Serializable {
    }

    @FunctionalInterface
    public interface ToLongFunction<T> extends java.util.function.ToLongFunction<T>, Serializable {
    }

    @FunctionalInterface
    public interface UnaryOperator<T> extends Function<T, T>, java.util.function.UnaryOperator<T>, Serializable {
    }

    @FunctionalInterface
    public interface IntUnaryOperator extends java.util.function.IntUnaryOperator, Serializable {
        static IntUnaryOperator identity() {
            return t -> t;
        }
    }

    @FunctionalInterface
    public interface LongUnaryOperator extends java.util.function.LongUnaryOperator, Serializable {
        static LongUnaryOperator identity() {
            return t -> t;
        }
    }

    @FunctionalInterface
    public interface DoubleUnaryOperator extends java.util.function.DoubleUnaryOperator, Serializable {
        static DoubleUnaryOperator identity() {
            return t -> t;
        }
    }

    @FunctionalInterface
    public interface Comparator<T> extends java.util.Comparator<T>, Serializable {

        @SuppressWarnings("unchecked")
        static <T extends Comparable<? super T>> Comparator<T> naturalOrder() {
            return (Comparator<T>) DistributedComparators.NATURAL_ORDER_COMPARATOR;
        }
    }

    public interface Collector<T, A, R> extends java.util.stream.Collector<T, A, R>, Serializable {

        @Override
        Supplier<A> supplier();

        @Override
        BiConsumer<A, T> accumulator();

        @Override
        BinaryOperator<A> combiner();

        @Override
        Function<A, R> finisher();

        static <T, R> Collector<T, R, R> of(Supplier<R> supplier,
                                            BiConsumer<R, T> accumulator,
                                            BinaryOperator<R> combiner,
                                            Characteristics... characteristics) {
            Objects.requireNonNull(supplier);
            Objects.requireNonNull(accumulator);
            Objects.requireNonNull(combiner);
            Objects.requireNonNull(characteristics);
            Set<Characteristics> cs = (characteristics.length == 0)
                    ? DistributedCollectors.CH_ID
                    : Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH,
                    characteristics));
            return new DistributedCollectorImpl<>(supplier, accumulator, combiner, cs);
        }

        static <T, A, R> Collector<T, A, R> of(Supplier<A> supplier,
                                               BiConsumer<A, T> accumulator,
                                               BinaryOperator<A> combiner,
                                               Function<A, R> finisher,
                                               Characteristics... characteristics) {
            Objects.requireNonNull(supplier);
            Objects.requireNonNull(accumulator);
            Objects.requireNonNull(combiner);
            Objects.requireNonNull(finisher);
            Objects.requireNonNull(characteristics);
            Set<Characteristics> cs = DistributedCollectors.CH_NOID;
            if (characteristics.length > 0) {
                cs = EnumSet.noneOf(Characteristics.class);
                Collections.addAll(cs, characteristics);
                cs = Collections.unmodifiableSet(cs);
            }
            return new DistributedCollectorImpl<>(supplier, accumulator, combiner, finisher, cs);
        }

        R collect(StreamContext context, Pipeline<? extends T> upstream);
    }

    public static class IntSummaryStatistics extends java.util.IntSummaryStatistics implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(this.getCount());
            out.writeLong(this.getSum());
            out.writeInt(this.getMin());
            out.writeInt(this.getMax());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            try {
                Class<?> clazz = java.util.IntSummaryStatistics.class;
                setPrivateField(this, clazz, "count", in.readLong());
                setPrivateField(this, clazz, "sum", in.readLong());
                setPrivateField(this, clazz, "min", in.readInt());
                setPrivateField(this, clazz, "max", in.readInt());
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw StreamUtil.reThrow(e);
            }
        }
    }

    public static class DoubleSummaryStatistics extends java.util.DoubleSummaryStatistics implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(this.getCount());
            out.writeDouble(this.getSum());
            out.writeDouble(this.getMin());
            out.writeDouble(this.getMax());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            try {
                Class<?> clazz = java.util.DoubleSummaryStatistics.class;
                setPrivateField(this, clazz, "count", in.readLong());
                setPrivateField(this, clazz, "sum", in.readDouble());
                setPrivateField(this, clazz, "min", in.readDouble());
                setPrivateField(this, clazz, "max", in.readDouble());
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw StreamUtil.reThrow(e);
            }
        }
    }

    public static class LongSummaryStatistics extends java.util.LongSummaryStatistics implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(this.getCount());
            out.writeLong(this.getSum());
            out.writeLong(this.getMin());
            out.writeLong(this.getMax());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            try {
                Class<?> clazz = java.util.LongSummaryStatistics.class;
                setPrivateField(this, clazz, "count", in.readLong());
                setPrivateField(this, clazz, "sum", in.readLong());
                setPrivateField(this, clazz, "min", in.readLong());
                setPrivateField(this, clazz, "max", in.readLong());
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw StreamUtil.reThrow(e);
            }
        }
    }

    /**
     * A container object which may or may not contain a non-null value.
     * If a value is present, {@code isPresent()} will return {@code true} and
     * {@code get()} will return the value.
     *
     * <p>Additional methods that depend on the presence or absence of a contained
     * value are provided, such as {@link #orElse(java.lang.Object) orElse()}
     * (return a default value if value not present) and
     * {@link #ifPresent(java.util.function.Consumer) ifPresent()} (execute a block
     * of code if the value is present).
     *
     * <p>This is a <a href="../lang/doc-files/ValueBased.html">value-based</a>
     * class; use of identity-sensitive operations (including reference equality
     * ({@code ==}), identity hash code, or synchronization) on instances of
     * {@code Optional} may have unpredictable results and should be avoided.
     *
     * @since 1.8
     */

    //TODO: find another solution rather than copy paste?
    public static final class Optional<T> implements Serializable {
        /**
         * Common instance for {@code empty()}.
         */
        private static final Optional<?> EMPTY = new Optional<>();

        /**
         * If non-null, the value; if null, indicates no value is present
         */
        private final T value;

        /**
         * Constructs an empty instance.
         *
         * @implNote Generally only one empty instance, {@link Optional#EMPTY},
         * should exist per VM.
         */
        private Optional() {
            this.value = null;
        }

        /**
         * Constructs an instance with the value present.
         *
         * @param value the non-null value to be present
         * @throws NullPointerException if value is null
         */
        private Optional(T value) {
            this.value = Objects.requireNonNull(value);
        }

        /**
         * Returns an empty {@code Optional} instance.  No value is present for this
         * Optional.
         *
         * @param <T> Type of the non-existent value
         * @return an empty {@code Optional}
         * @apiNote  Though it may be tempting to do so, avoid testing if an object
         * is empty by comparing with {@code ==} against instances returned by
         * {@code Option.empty()}. There is no guarantee that it is a singleton.
         * Instead, use {@link #isPresent()}.
         */
        public static <T> Optional<T> empty() {
            @SuppressWarnings("unchecked")
            Optional<T> t = (Optional<T>) EMPTY;
            return t;
        }

        /**
         * Returns an {@code Optional} with the specified present non-null value.
         *
         * @param <T>   the class of the value
         * @param value the value to be present, which must be non-null
         * @return an {@code Optional} with the value present
         * @throws NullPointerException if value is null
         */
        public static <T> Optional<T> of(T value) {
            return new Optional<>(value);
        }

        /**
         * Returns an {@code Optional} describing the specified value, if non-null,
         * otherwise returns an empty {@code Optional}.
         *
         * @param <T>   the class of the value
         * @param value the possibly-null value to describe
         * @return an {@code Optional} with a present value if the specified value
         * is non-null, otherwise an empty {@code Optional}
         */
        public static <T> Optional<T> ofNullable(T value) {
            return value == null ? empty() : of(value);
        }

        /**
         * If a value is present in this {@code Optional}, returns the value,
         * otherwise throws {@code NoSuchElementException}.
         *
         * @return the non-null value held by this {@code Optional}
         * @throws NoSuchElementException if there is no value present
         * @see Optional#isPresent()
         */
        public T get() {
            if (value == null) {
                throw new NoSuchElementException("No value present");
            }
            return value;
        }

        /**
         * Return {@code true} if there is a value present, otherwise {@code false}.
         *
         * @return {@code true} if there is a value present, otherwise {@code false}
         */
        public boolean isPresent() {
            return value != null;
        }

        /**
         * If a value is present, invoke the specified consumer with the value,
         * otherwise do nothing.
         *
         * @param consumer block to be executed if a value is present
         * @throws NullPointerException if value is present and {@code consumer} is
         *                              null
         */
        public void ifPresent(java.util.function.Consumer<? super T> consumer) {
            if (value != null) {
                consumer.accept(value);
            }
        }

        /**
         * If a value is present, and the value matches the given predicate,
         * return an {@code Optional} describing the value, otherwise return an
         * empty {@code Optional}.
         *
         * @param predicate a predicate to apply to the value, if present
         * @return an {@code Optional} describing the value of this {@code Optional}
         * if a value is present and the value matches the given predicate,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the predicate is null
         */
        public Optional<T> filter(java.util.function.Predicate<? super T> predicate) {
            Objects.requireNonNull(predicate);
            if (!isPresent()) {
                return this;
            } else {
                return predicate.test(value) ? this : empty();
            }
        }

        /**
         * If a value is present, apply the provided mapping function to it,
         * and if the result is non-null, return an {@code Optional} describing the
         * result.  Otherwise return an empty {@code Optional}.
         *
         * @param <U>    The type of the result of the mapping function
         * @param mapper a mapping function to apply to the value, if present
         * @return an {@code Optional} describing the result of applying a mapping
         * function to the value of this {@code Optional}, if a value is present,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the mapping function is null
         * @apiNote This method supports post-processing on optional values, without
         * the need to explicitly check for a return status.  For example, the
         * following code traverses a stream of file names, selects one that has
         * not yet been processed, and then opens that file, returning an
         * {@code Optional<FileInputStream>}:
         *
         * <pre>{@code
         *     Optional<FileInputStream> fis =
         *         names.stream().filter(name -> !isProcessedYet(name))
         *                       .findFirst()
         *                       .map(name -> new FileInputStream(name));
         * }</pre>
         * <p>
         * Here, {@code findFirst} returns an {@code Optional<String>}, and then
         * {@code map} returns an {@code Optional<FileInputStream>} for the desired
         * file if one exists.
         */
        public <U> Optional<U> map(java.util.function.Function<? super T, ? extends U> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent()) {
                return empty();
            } else {
                return Optional.ofNullable(mapper.apply(value));
            }
        }

        /**
         * If a value is present, apply the provided {@code Optional}-bearing
         * mapping function to it, return that result, otherwise return an empty
         * {@code Optional}.  This method is similar to {@link #map(java.util.function.Function)},
         * but the provided mapper is one whose result is already an {@code Optional},
         * and if invoked, {@code flatMap} does not wrap it with an additional
         * {@code Optional}.
         *
         * @param <U>    The type parameter to the {@code Optional} returned by
         * @param mapper a mapping function to apply to the value, if present
         *               the mapping function
         * @return the result of applying an {@code Optional}-bearing mapping
         * function to the value of this {@code Optional}, if a value is present,
         * otherwise an empty {@code Optional}
         * @throws NullPointerException if the mapping function is null or returns
         *                              a null result
         */
        public <U> Optional<U> flatMap(java.util.function.Function<? super T, Optional<U>> mapper) {
            Objects.requireNonNull(mapper);
            if (!isPresent()) {
                return empty();
            } else {
                return Objects.requireNonNull(mapper.apply(value));
            }
        }

        /**
         * Return the value if present, otherwise return {@code other}.
         *
         * @param other the value to be returned if there is no value present, may
         *              be null
         * @return the value, if present, otherwise {@code other}
         */
        public T orElse(T other) {
            return value != null ? value : other;
        }

        /**
         * Return the value if present, otherwise invoke {@code other} and return
         * the result of that invocation.
         *
         * @param other a {@code Supplier} whose result is returned if no value
         *              is present
         * @return the value if present otherwise the result of {@code other.get()}
         * @throws NullPointerException if value is not present and {@code other} is
         *                              null
         */
        public T orElseGet(java.util.function.Supplier<? extends T> other) {
            return value != null ? value : other.get();
        }

        /**
         * Return the contained value, if present, otherwise throw an exception
         * to be created by the provided supplier.
         *
         * @param <X>               Type of the exception to be thrown
         * @param exceptionSupplier The supplier which will return the exception to
         *                          be thrown
         * @return the present value
         * @throws X                    if there is no value present
         * @throws NullPointerException if no value is present and
         *                              {@code exceptionSupplier} is null
         * @apiNote A method reference to the exception constructor with an empty
         * argument list can be used as the supplier. For example,
         * {@code IllegalStateException::new}
         */
        public <X extends Throwable> T orElseThrow(java.util.function.Supplier<? extends X> exceptionSupplier) throws X {
            if (value != null) {
                return value;
            } else {
                throw exceptionSupplier.get();
            }
        }

        /**
         * Indicates whether some other object is "equal to" this Optional. The
         * other object is considered equal if:
         * <ul>
         * <li>it is also an {@code Optional} and;
         * <li>both instances have no value present or;
         * <li>the present values are "equal to" each other via {@code equals()}.
         * </ul>
         *
         * @param obj an object to be tested for equality
         * @return {code true} if the other object is "equal to" this object
         * otherwise {@code false}
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof Optional)) {
                return false;
            }

            Optional<?> other = (Optional<?>) obj;
            return Objects.equals(value, other.value);
        }

        /**
         * Returns the hash code value of the present value, if any, or 0 (zero) if
         * no value is present.
         *
         * @return hash code value of the present value or 0 if no value is present
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }

        /**
         * Returns a non-empty string representation of this Optional suitable for
         * debugging. The exact presentation format is unspecified and may vary
         * between implementations and versions.
         *
         * @return the string representation of this instance
         * @implSpec If a value is present the result must include its string
         * representation in the result. Empty and present Optionals must be
         * unambiguously differentiable.
         */
        @Override
        public String toString() {
            return value != null
                    ? String.format("Optional[%s]", value)
                    : "Optional.empty";
        }
    }

}
