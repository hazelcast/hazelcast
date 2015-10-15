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
 * It sounds counter-intuitive, but a single extract can result
 * in multiple values when arrays or collections are involved.
 * <p/>
 * Example 1: I have a class Body containing a collection of limbs.
 * Then extraction of attribute <code>limb[*].name</code> results
 * in MultiResult where each result contains a name of a limb.
 * <p/>
 * Example 2: If I used just <code>limb[*]</code> then I would
 * get a MultiResult as well - each result would represent a limb.
 * <p/>
 * Example 3: If I used <code>limb</code> as attribute
 * (without the modifier) then the result of extraction WOULD
 * not be a MultiResult. It would be just a single result -
 * - the single collection of all limbs.
 */
public final class MultiResult {

    private List<Object> results;

    public MultiResult() {
        this.results = new ArrayList<Object>();
    }

    public void add(Object result) {
        results.add(result);
    }

    public List<Object> getResults() {
        return results;
    }

    public boolean isEmpty() {
        return results.isEmpty();
    }

}
