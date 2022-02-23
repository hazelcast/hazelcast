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

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Set;

/**
 * Extends the {@link Predicate} interface with the ability to support indexes.
 * <p>
 * The general contract is:
 * <ul>
 * <li>While performing a query, the query engine invokes the {@link #isIndexed}
 * method of the index-aware predicate and provides it with the
 * {@link QueryContext} instance. The predicate may consult the passed query
 * context regarding the indexes configured for the map in question and decide
 * whether it is possible to use the indexes to speed up the query processing or
 * not.
 * <li>If the predicate decides to use the indexes, its {@link #filter} method
 * is invoked by the query engine to obtain a filtered entry set. The query
 * engine assumes that this obtained result set is consistent with the entry set
 * at the time the result set was produced. In other words, all the entries for
 * which the predicate {@link #apply evaluates} to {@code true} are included in
 * the result set and all the entries for which the predicate evaluates to
 * {@code false} are excluded from the result set.
 * <li>If the predicate decides <i>not</i> to use indexes, the query engine
 * produces the result set on its own and evaluates the predicate for every
 * entry by invoking the {@link #apply} method of the predicate.
 * </ul>
 *
 * @param <K> the type of keys the predicate operates on.
 * @param <V> the type of values the predicate operates on.
 */
@BinaryInterface
public interface IndexAwarePredicate<K, V> extends Predicate<K, V> {

    /**
     * Produces a filtered entry set by utilizing the indexes available while
     * executing the query in the given query context.
     * <p>
     * The query engine assumes this method produces the result set faster than
     * a simple evaluation of the predicate on every entry.
     *
     * @param queryContext        the query context to access the indexes. The passed
     *                            query context is valid only for a duration of a single
     *                            call to the method.
     * @return the produced filtered entry set.
     */
    Set<QueryableEntry<K, V>> filter(QueryContext queryContext);

    /**
     * Signals to the query engine that this predicate is able to utilize the
     * indexes available while executing the query in the given query context.
     *
     * @param queryContext        the query context to consult for the available
     *                            indexes.
     * @return {@code true} if this predicate is able to use the indexes to
     * speed up the processing, {@code false} otherwise.
     */
    boolean isIndexed(QueryContext queryContext);
}
