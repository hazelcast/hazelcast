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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;

/**
 * Interface for predicates which operate on an array of sub-predicates.
 * Implementations of this interface must include a default no-args constructor.
 * If a {@code CompoundPredicate} is also a {@link VisitablePredicate}, taking into account
 * the immutability requirements for {@code VisitablePredicate}s, {@link #setPredicates(Predicate[])} should
 * throw an {@code IllegalStateException} when invoked on a {@code CompoundStatement} whose sub-predicates
 * have already been set.
 */
public interface CompoundPredicate {

    /**
     * An array of predicates which compose this predicate.
     * @return array of predicates which will be applied by this predicate
     */
    <K, V> Predicate<K, V>[] getPredicates();

    /**
     * Set the sub-predicates of this {@code CompoundPredicate}. If a predicate should be treated as effectively
     * immutable, such as a {@link VisitablePredicate}, it is advised that this method throws
     * {@link IllegalStateException} when sub-predicates have already been defined.
     * @param predicates
     * @param <K>
     * @param <V>
     */
    <K, V> void setPredicates(Predicate<K, V>[] predicates);

}
