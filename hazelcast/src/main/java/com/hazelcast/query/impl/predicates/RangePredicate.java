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
 * Defines a common contract for range-like predicates.
 */
public interface RangePredicate extends Predicate {

    /**
     * @return the attribute on which this range predicate acts.
     */
    String getAttribute();

    /**
     * @return the lower/left bound of this range predicate or {@code null} if
     * the predicate range is left-unbounded.
     */
    Comparable getFrom();

    /**
     * @return {@code true} if the predicate range is left-closed, {@code false}
     * otherwise.
     * <p>
     * Can't be {@code true} if the predicate range is left-unbounded.
     */
    boolean isFromInclusive();

    /**
     * @return the upper/right bound of this range predicate or {@code null} if
     * the predicate range is right-unbounded.
     */
    Comparable getTo();

    /**
     * @return {@code true} if the predicate range is right-closed, {@code false}
     * otherwise.
     * <p>
     * Can't be {@code true} if the predicate range is right-unbounded.
     */
    boolean isToInclusive();

}
