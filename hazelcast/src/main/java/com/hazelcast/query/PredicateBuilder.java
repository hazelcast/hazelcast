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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PredicateBuilder implements Predicate, IndexAwarePredicate {
    Expression exp = null;
    List<Predicate> lsPredicates = new ArrayList<Predicate>();

    public boolean apply(MapEntry mapEntry) {
        return lsPredicates.get(0).apply(mapEntry);
    }

    public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index> mapIndexes) {
        boolean strong = true;
        Predicate predicate = lsPredicates.get(0);
        if (predicate instanceof IndexAwarePredicate) {
            IndexAwarePredicate p = (IndexAwarePredicate) predicate;
            if (!p.collectIndexAwarePredicates(lsIndexPredicates, mapIndexes)) {
                strong = false;
            }
        } else {
            strong = false;
        }
        return strong;
    }

    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }

    public Set<MapEntry> filter(QueryContext queryContext) {
        return null;
    }

    public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index> mapIndexes) {
        Predicate predicate = lsPredicates.get(0);
        if (predicate instanceof IndexAwarePredicate) {
            IndexAwarePredicate p = (IndexAwarePredicate) predicate;
            p.collectAppliedIndexes(setAppliedIndexes, mapIndexes);
        }
    }

    public EntryObject getEntryObject() {
        return new EntryObject(this);
    }

    public PredicateBuilder and(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.and(first, second));
        return this;
    }

    public PredicateBuilder or(Predicate predicate) {
        int index = lsPredicates.size() - 2;
        Predicate first = lsPredicates.remove(index);
        Predicate second = lsPredicates.remove(index);
        lsPredicates.add(Predicates.or(first, second));
        return this;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("QueryBuilder");
        sb.append("{\n");
        sb.append(lsPredicates.get(0));
        sb.append("\n}");
        return sb.toString();
    }
}
