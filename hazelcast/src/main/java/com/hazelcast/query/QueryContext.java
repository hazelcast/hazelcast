/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;

import java.util.Map;
import java.util.Set;

public class QueryContext {
    boolean strong = false;
    Predicate predicate = null;
    String mapName;
    Set<MapEntry> results = null;
    int indexedPredicateCount = 0;
    Map<Expression, Index> mapIndexes = null;

    public QueryContext(String mapName, Predicate predicate) {
        this.mapName = mapName;
        this.predicate = predicate;
    }

    public int getIndexedPredicateCount() {
        return indexedPredicateCount;
    }

    public void setIndexedPredicateCount(int indexedPredicateCount) {
        this.indexedPredicateCount = indexedPredicateCount;
    }

    public boolean isStrong() {
        return strong;
    }

    public void setStrong(boolean strong) {
        this.strong = strong;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public String getMapName() {
        return mapName;
    }

    public void setMapName(String mapName) {
        this.mapName = mapName;
    }

    public Set<MapEntry> getResults() {
        return results;
    }

    public void setResults(Set<MapEntry> results) {
        this.results = results;
    }

    public void setMapIndexes(Map<Expression, Index> mapIndexes) {
        this.mapIndexes = mapIndexes;
    }

    public Map<Expression, Index> getMapIndexes() {
        return mapIndexes;
    }
}
