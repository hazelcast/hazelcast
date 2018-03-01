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

package com.hazelcast.jet;

import com.hazelcast.query.Predicate;

/**
 * Generic wrappers for methods in {@link com.hazelcast.query.Predicates}.
 * These classes match 1:1.
 */
@SuppressWarnings("unchecked")
public final class GenericPredicates {
    private GenericPredicates() {
    }

    /**
     * See {@link com.hazelcast.query.Predicates#alwaysTrue()}.
     */
    public static <K, V> Predicate<K, V> alwaysTrue() {
        return com.hazelcast.query.Predicates.alwaysTrue();
    }

    /**
     * See {@link com.hazelcast.query.Predicates#alwaysFalse()}.
     */
    public static <K, V> Predicate<K, V> alwaysFalse() {
        return com.hazelcast.query.Predicates.alwaysFalse();
    }

    /**
     * See {@link com.hazelcast.query.Predicates#instanceOf(Class)}.
     */
    public static <K, V> Predicate<K, V> instanceOf(final Class<? extends V> klass) {
        return com.hazelcast.query.Predicates.instanceOf(klass);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#and(Predicate[])}.
     */
    public static <K, V> Predicate<K, V> and(Predicate<K, V> ... predicates) {
        return com.hazelcast.query.Predicates.and(predicates);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#not(Predicate)}.
     */
    public static <K, V> Predicate<K, V> not(Predicate<K, V> predicate) {
        return com.hazelcast.query.Predicates.not(predicate);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#or(Predicate[])}.
     */
    public static <K, V> Predicate<K, V> or(Predicate<K, V> ... predicates) {
        return com.hazelcast.query.Predicates.or(predicates);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#notEqual(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> notEqual(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.notEqual(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#equal(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> equal(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.equal(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#like(String, String)}.
     */
    public static <K, V> Predicate<K, V> like(String attribute, String pattern) {
        return com.hazelcast.query.Predicates.like(attribute, pattern);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#ilike(String, String)}.
     */
    public static <K, V> Predicate<K, V> ilike(String attribute, String pattern) {
        return com.hazelcast.query.Predicates.ilike(attribute, pattern);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#regex(String, String)}.
     */
    public static <K, V> Predicate<K, V> regex(String attribute, String pattern) {
        return com.hazelcast.query.Predicates.regex(attribute, pattern);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#greaterThan(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> greaterThan(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.greaterThan(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#greaterEqual(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> greaterEqual(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.greaterEqual(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#lessThan(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> lessThan(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.lessThan(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#lessEqual(String, Comparable)}.
     */
    public static <K, V> Predicate<K, V> lessEqual(String attribute, Comparable value) {
        return com.hazelcast.query.Predicates.lessEqual(attribute, value);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#between(String, Comparable, Comparable)}.
     */
    public static <K, V> Predicate<K, V> between(String attribute, Comparable from, Comparable to) {
        return com.hazelcast.query.Predicates.between(attribute, from, to);
    }

    /**
     * See {@link com.hazelcast.query.Predicates#in(String, Comparable[])}.
     */
    public static <K, V> Predicate<K, V> in(String attribute, Comparable... values) {
        return com.hazelcast.query.Predicates.in(attribute, values);
    }
}
