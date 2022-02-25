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

package com.hazelcast.query.impl.getters;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents multiple results from a single attribute extraction.
 * <p>
 * MultiResult is an aggregate of results that is returned if the ValueExtractor returns multiple results due to a
 * reduce operation executed on a hierarchy of values.
 * <p>
 * It sounds counter-intuitive, but a single extraction may return multiple values when arrays or collections are
 * involved.
 * <p>
 * Let's have a look at the following data structure:
 * <pre>
 * class Swap {
 *     Leg legs[2];
 * }
 * class Leg {
 *     String currency;
 * }
 * </pre>
 * <p>
 * The following extraction of the currency attribute <code>legs[any].currency</code> results in two currencies for each
 * Leg. In order to return both values in one result of the extract operation both currencies are returned in a
 * single MultiResult object where each result contains a name of the currency.
 * It allows the user to operate on multiple "reduced" values as if they were single-values.
 * <p>
 * Let's have a look at the following queries:
 * <ul>
 * <li>leg[1].currency   = 'EUR'</li>
 * <li>leg[any].currency = 'EUR'</li>
 * </ul>
 * In the first query, the extraction will return just one currency, whereas the extraction in the second query will
 * return a MultiResult containing two currencies.
 * During the evaluation of the "=" equals predicate the MultiResult will be "unfolded" and the condition will
 * evaluated against all currencies from the MultiResult. If there is "any" currency that matches the condition the
 * whole predicate will be evaluated to "true" and the matching Swap will be returned.
 * As a result all Swaps will be returned where there's at lease one Leg with EUR currency.
 * <p>
 * Other examples:
 * <ul>
 * <li>legs -&gt; returns one 'single-value' result -&gt; a collection of values</li>
 * <li>legs[0] -&gt; returns one 'single result' - a Leg object</li>
 * <li>legs[0].currency -&gt; returns one 'multi value' result - an array of Legs</li>
 * <li>legs[any] -&gt; returns a MultiResult - that contains a collection of Leg objects</li>
 * <li>legs[any].currency -&gt; returns a MultiResult - that contains a collection of String objects</li>
 * </ul>
 * @param <T> type of the underlying result store in the MultiResult
 */
public class MultiResult<T> {

    private List<T> results;

    /**
     * Indicates that the result of a multi-result evaluation using [any] operator
     * didn't reach any data since the field where the [any] operator is used was null or empty.
     *
     * Allows differentiating whether there is a null value in the MultiResult
     * that is a result of a null or empty target in the expression.
     *
     * For query evaluation the difference is not important - and we need to return null in both cases there.
     * For aggregations it makes a difference and we need this context knowledge there.
     */
    private boolean nullOrEmptyTarget;

    public MultiResult() {
        this.results = new ArrayList<T>();
    }

    public MultiResult(List<T> results) {
        this.results = results;
    }

    /**
     * @param result result to be added to this MultiResult
     */
    public void add(T result) {
        results.add(result);
    }

    public void addNullOrEmptyTarget() {
        if (!nullOrEmptyTarget) {
            // we don't want to store more than one null if we reach a null/empty target
            // it's enough to have one for the query evaluation (it's for null matching)
            nullOrEmptyTarget = true;
            results.add(null);
        }
    }

    /**
     * @return a mutable underlying list of collected results
     */
    public List<T> getResults() {
        return results;
    }

    /**
     * @return true if the MultiResult is empty; false otherwise
     */
    public boolean isEmpty() {
        return results.isEmpty();
    }

    public boolean isNullEmptyTarget() {
        return nullOrEmptyTarget;
    }

    public void setNullOrEmptyTarget(boolean nullOrEmptyTarget) {
        this.nullOrEmptyTarget = nullOrEmptyTarget;
    }

}
