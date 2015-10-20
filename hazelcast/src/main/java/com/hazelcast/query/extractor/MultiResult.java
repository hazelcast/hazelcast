/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.extractor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents multiple results from a single attribute extraction.
 * <p/>
 * MultiResult is an aggregate of results that is returned if the ValueExtractor returns multiple results due to a
 * reduce operation executed on a hierarchy of values.
 * <p/>
 * It sounds counter-intuitive, but a single extraction may return multiple values when arrays or collections are
 * involved.
 * <p/>
 * Let's have a look at the following data structure:
 * <code>
 * class Swap {
 * Leg legs[2];
 * }
 * <p/>
 * class Leg {
 * String currency;
 * }
 * </code>
 * <p/>
 * The following extraction of the currency attribute <code>legs[any].currency</code> results in two currencies for each
 * Leg. In order to return both values in one result of the extract operation both currencies are returned in a
 * single MultiResult object where each result contains a name of the currency.
 * It allows the user to operate on multiple "reduced" values as if they were single-values.
 * <p/>
 * Let's have a look at the following queries:
 * <ul>
 * <li>leg[1].currency   = 'EUR'</li>
 * <li>leg[any].currency = 'EUR'</li>
 * <p/>
 * </ul>
 * In the first query, the extraction will return just one currency, whereas the extraction in the second query will
 * return a MultiResult containing two currencies.
 * During the evaluation of the "=" equals predicate the MultiResult will be "unfolded" and the condition will
 * evaluated against all currencies from the MultiResult. If there is "any" currency that matches the condition the
 * whole predicate will be evaluated to "true" and the matching Swap will be returned.
 * As a result all Swaps will be returned where there's at lease one Leg with EUR currency.
 * <p/>
 * Other examples:
 * legs -> returns one 'single-value' result -> a collection of values
 * legs[0] -> returns one 'single result' - a Leg object
 * legs[0].currency -> returns one 'multi value' result - an array of Legs
 * legs[any] -> returns a MultiResult - that contains a collection of Leg objects
 * legs[any].currency -> returns a MultiResult - that contains a collection of String objects
 *
 * @param <T> type of the underlying result store in the MultiResult
 */
public final class MultiResult<T> {

    private List<T> results;

    public MultiResult() {
        this.results = new ArrayList<T>();
    }

    /**
     * @param result result to be added to this MultiResult
     */
    public void add(T result) {
        results.add(result);
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

}
