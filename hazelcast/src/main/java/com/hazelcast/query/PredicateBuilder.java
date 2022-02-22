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

package com.hazelcast.query;

import com.hazelcast.internal.serialization.BinaryInterface;

/**
 * This interface provides functionality to build a predicate.
 */
@BinaryInterface
public interface PredicateBuilder extends Predicate {

    String getAttribute();

    void setAttribute(String attribute);

    EntryObject getEntryObject();

    PredicateBuilder and(Predicate predicate);

    PredicateBuilder or(Predicate predicate);

    /**
     * This interface provides entry-level functionality related to building a predicate.
     */
    interface EntryObject {
        EntryObject get(String attribute);

        EntryObject key();

        PredicateBuilder is(String attribute);

        PredicateBuilder isNot(String attribute);

        PredicateBuilder equal(Comparable value);

        PredicateBuilder notEqual(Comparable value);

        PredicateBuilder isNull();

        PredicateBuilder isNotNull();

        PredicateBuilder greaterThan(Comparable value);

        PredicateBuilder greaterEqual(Comparable value);

        PredicateBuilder lessThan(Comparable value);

        PredicateBuilder lessEqual(Comparable value);

        PredicateBuilder between(Comparable from, Comparable to);

        PredicateBuilder in(Comparable... values);
    }

}
