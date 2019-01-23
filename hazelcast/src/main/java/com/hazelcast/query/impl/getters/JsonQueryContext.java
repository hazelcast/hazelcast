/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.json.internal.JsonPattern;
import com.hazelcast.util.collection.WeightedEvictableList;
import com.hazelcast.util.collection.WeightedEvictableList.WeightedItem;

import java.util.List;

public class JsonQueryContext {

    private static final int PATTERN_CACHE_MAX_SIZE = 20;
    private static final int PATTERN_CACHE_MAX_VOTES = 400;

    private JsonPathCursor pathCursor;
    private WeightedEvictableList<JsonPattern> patternList;

    public JsonQueryContext(String attributePath) {
        this.pathCursor = JsonPathCursor.createCursor(attributePath);
        patternList = new WeightedEvictableList<JsonPattern>(PATTERN_CACHE_MAX_SIZE, PATTERN_CACHE_MAX_VOTES);
    }

    public List<WeightedItem<JsonPattern>> getPatternListSnapshot() {
        return patternList.getSnapshot();
    }

    public void voteFor(WeightedItem<JsonPattern> item) {
        patternList.voteFor(item);
    }

    public WeightedItem<JsonPattern> addPattern(JsonPattern pattern) {
        return patternList.add(pattern);
    }

    public JsonPathCursor newJsonPathCursor() {
        return new JsonPathCursor(pathCursor);
    }

}
