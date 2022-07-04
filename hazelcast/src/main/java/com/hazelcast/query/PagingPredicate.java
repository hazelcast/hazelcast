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

import java.util.Comparator;
import java.util.Map;

/**
 * This interface is a special Predicate which helps to get a page-by-page result of a query.
 * It can be constructed with a page-size, an inner predicate for filtering, and a comparator for sorting.
 * This class is not thread-safe and stateless. To be able to reuse for another query, one should call
 * {@link #reset()}.
 * <br>
 * Here is an example usage:
 * <pre>
 * Predicate lessEqualThanFour = Predicates.lessEqual("this", 4);
 *
 * // We are constructing our paging predicate with a predicate and a page size. In this case query results
 * // are fetched in batches of two.
 * PagingPredicate predicate = Predicates.pagingPredicate(lessEqualThanFour, 2);
 *
 * // we are initializing our map with integers from 0 to 10 as keys and values.
 * IMap map = hazelcastInstance.getMap(...);
 * for (int i = 0; i &lt; 10; i++) {
 * map.put(i, i);
 * }
 *
 * // invoking the query
 * Collection&lt;Integer&gt; values = map.values(predicate);
 * System.out.println("values = " + values) // will print 'values = [0, 1]'
 * predicate.nextPage(); // we are setting up paging predicate to fetch the next page in the next call.
 * values = map.values(predicate);
 * System.out.println("values = " + values);// will print 'values = [2, 3]'
 * Entry anchor = predicate.getAnchor();
 * System.out.println("anchor -&gt; " + anchor); // will print 'anchor -&gt; 1=1',  since the anchor is the last entry of
 *                                               // the previous page.
 * predicate.previousPage(); // we are setting up paging predicate to fetch previous page in the next call
 * values = map.values(predicate);
 * System.out.println("values = " + values) // will print 'values = [0, 1]'
 * </pre>
 *
 * @param <K> type of the entry key
 * @param <V> type of the entry value
 * @see Predicates#pagingPredicate(int)
 */
@BinaryInterface
public interface PagingPredicate<K, V> extends Predicate<K, V> {

    /**
     * Resets for reuse.
     */
    void reset();

    /**
     * Sets the page value to next page.
     */
    void nextPage();

    /**
     * Sets the page value to previous page.
     */
    void previousPage();

    /**
     * Returns the current page value.
     * @return the current page value.
     */
    int getPage();

    /**
     * Sets the current page value.
     * @param page the current page value.
     */
    void setPage(int page);

    /**
     * Returns the page size.
     * @return the page size
     */
    int getPageSize();

    /**
     * Returns the comparator used by this predicate (if any).
     * @return the comparator or {@code null}
     */
    Comparator<Map.Entry<K, V>> getComparator();

    /**
     * Retrieve the anchor object which is the last value object on the previous page.
     * <p>
     * Note: This method will return `null` on the first page of the query result or if the predicate was not applied
     * for the previous page number.
     *
     * @return Map.Entry the anchor object which is the last value object on the previous page
     */
    Map.Entry<K, V> getAnchor();

}
