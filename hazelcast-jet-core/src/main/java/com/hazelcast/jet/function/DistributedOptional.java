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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code Serializable} variant of {@link Optional java.util.Optional}.
 */
public final class DistributedOptional<T> implements Serializable {

    private static final DistributedOptional<?> EMPTY = new DistributedOptional<>();

    private final T value;

    private DistributedOptional() {
        this.value = null;
    }

    private DistributedOptional(T value) {
        this.value = Objects.requireNonNull(value);
    }

    /**
     * {@code Serializable} variant of {@link Optional#empty()
     * java.util.Optional#empty()}.
     */
    public static <T> DistributedOptional<T> empty() {
        @SuppressWarnings("unchecked")
        DistributedOptional<T> t = (DistributedOptional<T>) EMPTY;
        return t;
    }

    /**
     * {@code Serializable} variant of {@link Optional#of(T)
     * java.util.Optional#of(T)}.
     */
    public static <T> DistributedOptional<T> of(T value) {
        return new DistributedOptional<>(value);
    }

    /**
     * {@code Serializable} variant of {@link Optional#ofNullable(T)
     * java.util.Optional#ofNullable(T)}.
     */
    public static <T> DistributedOptional<T> ofNullable(T value) {
        return value == null ? empty() : of(value);
    }

    /**
     * {@code Serializable} variant of {@link Optional#get()
     * java.util.Optional#get()}.
     */
    public T get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }

    /**
     * {@code Serializable} variant of {@link Optional#isPresent()
     * java.util.Optional#isPresent()}.
     */
    public boolean isPresent() {
        return value != null;
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#ifPresent(java.util.function.Consumer) java.util.Optional#ifPresent(Consumer)}.
     */
    public void ifPresent(DistributedConsumer<? super T> consumer) {
        if (value != null) {
            consumer.accept(value);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#filter(java.util.function.Predicate) java.util.Optional#filter(Predicate)}.
     */
    public DistributedOptional<T> filter(DistributedPredicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        if (!isPresent()) {
            return this;
        } else {
            return predicate.test(value) ? this : empty();
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#map(java.util.function.Function) java.util.Optional#map(Function)}.
     */
    public <U> DistributedOptional<U> map(DistributedFunction<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper);
        if (!isPresent()) {
            return empty();
        } else {
            return DistributedOptional.ofNullable(mapper.apply(value));
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#flatMap(java.util.function.Function) java.util.Optional#flatMap(Function)}.
     */
    public <U> DistributedOptional<U> flatMap(DistributedFunction<? super T, DistributedOptional<U>> mapper) {
        Objects.requireNonNull(mapper);
        if (!isPresent()) {
            return empty();
        } else {
            return Objects.requireNonNull(mapper.apply(value));
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#orElseGet(java.util.function.Supplier)
     * java.util.Optional#orElseGet(Supplier)}.
     */
    public T orElseGet(DistributedSupplier<? extends T> other) {
        return value != null ? value : other.get();
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#orElse(T) java.util.Optional#orElse(T)}.
     */
    public T orElse(T other) {
        return value != null ? value : other;
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#orElseThrow(java.util.function.Supplier)
     * java.util.Optional#orElseThrow(Supplier)}.
     */
    public <X extends Throwable> T orElseThrow(DistributedSupplier<? extends X> exceptionSupplier) throws X {
        if (value != null) {
            return value;
        } else {
            throw exceptionSupplier.get();
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#equals(Object) java.util.Optional#equals(Object)}.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DistributedOptional)) {
            return false;
        }

        DistributedOptional<?> other = (DistributedOptional<?>) obj;
        return Objects.equals(value, other.value);
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#hashCode() java.util.Optional#hashCode()}.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    /**
     * {@code Serializable} variant of {@link
     * Optional#toString() java.util.Optional#toString()}.
     */
    @Override
    public String toString() {
        return value != null
                ? String.format("DistributedOptional[%s]", value)
                : "DistributedOptional.empty";
    }
}
