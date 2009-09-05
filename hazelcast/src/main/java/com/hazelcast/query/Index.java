/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import java.util.*;

public class Index<T> {
    final Map<Long, Set<T>> mapIndex;
    final Expression expression;
    final boolean ordered;
    volatile boolean strong = false;
    volatile boolean checkedStregth = false;

    public Index(Expression expression, boolean ordered) {
        this.ordered = ordered;
        this.expression = expression;
        this.mapIndex = (ordered) ? new TreeMap<Long, Set<T>>() : new HashMap<Long, Set<T>>(1000);
    }

    public Map<Long, Set<T>> getMapIndex() {
        return mapIndex;
    }

    public Expression getExpression() {
        return expression;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public boolean isStrong() {
        return strong;
    }

    public void setStrong(boolean strong) {
        this.strong = strong;
    }

    public long extractLongValue(Object value) {
        Object extractedValue = expression.getValue(value);
        if (extractedValue == null) {
            return Long.MAX_VALUE;
        } else {
            if (!checkedStregth) {
                if (extractedValue instanceof Boolean) {
                    strong = true;
                } else if (extractedValue instanceof Number) {
                    strong = !(extractedValue instanceof Double || extractedValue instanceof Float);
                }
                checkedStregth = true;
            }
            return QueryService.getLongValue(extractedValue);
        }

    }

    void addNewIndex(long value, T record) {
        Set<T> lsRecords = mapIndex.get(value);
        if (lsRecords == null) {
            lsRecords = new LinkedHashSet<T>();
            mapIndex.put(value, lsRecords);
        }
        lsRecords.add(record);
    }

    void updateIndex(long oldValue, long newValue, T record) {
        if (oldValue == Long.MIN_VALUE) {
            addNewIndex(newValue, record);
        } else {
            if (oldValue != newValue) {
                removeIndex(oldValue, record);
                addNewIndex(newValue, record);
            }
        }
    }

    void removeIndex(long value, T record) {
        Set<T> lsRecords = mapIndex.get(value);
        if (lsRecords != null && lsRecords.size() > 0) {
            lsRecords.remove(record);
            if (lsRecords.size() == 0) {
                mapIndex.remove(value);
            }
        }
    }

    Set<T> getRecords(long value) {
        return mapIndex.get(value);
    }

    Set<T> getSubRecords(boolean equal, boolean lessThan, long value) {
        TreeMap<Long, Set<T>> treeMap = (TreeMap<Long, Set<T>>) mapIndex;
        Set<T> results = new HashSet<T>();
        Map<Long, Set<T>> sub = (lessThan) ? treeMap.headMap(value) : treeMap.tailMap(value);
        Set<Map.Entry<Long, Set<T>>> entries = sub.entrySet();
        for (Map.Entry<Long, Set<T>> entry : entries) {
            if (equal || entry.getKey() != value) {
                results.addAll(entry.getValue());

            }
        }
        return results;
    }

    Set<T> getSubRecords(long from, long to) {
        TreeMap<Long, Set<T>> treeMap = (TreeMap<Long, Set<T>>) mapIndex;
        Set<T> results = new HashSet<T>();
        Collection<Set<T>> sub = treeMap.subMap(from, to).values();
        for (Set<T> records : sub) {
            results.addAll(records);
        }
        return results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Index index = (Index) o;

        if (!expression.equals(index.expression)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("Index");
        sb.append("{size='").append(mapIndex.size()).append('\'');
        sb.append(", ordered=").append(ordered);
        sb.append(", strong=").append(strong);
        sb.append(", expression=").append(expression.getClass());
        sb.append('}');
        return sb.toString();
    }


}