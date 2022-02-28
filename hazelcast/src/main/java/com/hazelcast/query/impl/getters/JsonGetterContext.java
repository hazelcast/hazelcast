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

import com.hazelcast.json.internal.JsonPattern;
import com.hazelcast.internal.util.collection.WeightedEvictableList;
import com.hazelcast.internal.util.collection.WeightedEvictableList.WeightedItem;

import java.util.List;

public class JsonGetterContext {

    private static final int PATTERN_CACHE_MAX_SIZE = 20;
    private static final int PATTERN_CACHE_MAX_VOTES = 20;

    private final JsonPathCursor pathCursor;
    private final ThreadLocal<WeightedEvictableList<JsonPattern>> patternListHolder;

    public JsonGetterContext(String attributePath) {
        this.pathCursor = JsonPathCursor.createCursor(attributePath);
        patternListHolder = new ThreadLocal<WeightedEvictableList<JsonPattern>>();
    }

    public List<WeightedItem<JsonPattern>> getPatternListSnapshot() {
        return getPatternList().getList();
    }

    public void voteFor(WeightedItem<JsonPattern> item) {
        getPatternList().voteFor(item);
    }

    public WeightedItem<JsonPattern> addOrVoteForPattern(JsonPattern pattern) {
        return getPatternList().addOrVote(pattern);
    }

    public JsonPathCursor newJsonPathCursor() {
        return new JsonPathCursor(pathCursor);
    }

    private WeightedEvictableList<JsonPattern> getPatternList() {
        WeightedEvictableList<JsonPattern> list = patternListHolder.get();
        if (list == null) {
            list = new WeightedEvictableList<JsonPattern>(PATTERN_CACHE_MAX_SIZE, PATTERN_CACHE_MAX_VOTES);
            patternListHolder.set(list);
        }
        return list;
    }
}
