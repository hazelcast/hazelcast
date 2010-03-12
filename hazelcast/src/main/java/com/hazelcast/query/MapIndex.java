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
import com.hazelcast.impl.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapIndex {
    // recordId -- indexValue
    private final ConcurrentMap<Long, Long> recordValues = new ConcurrentHashMap<Long, Long>(10000);
    // indexValue -- Map<recordId, Record>
    private final ConcurrentMap<Long, ConcurrentMap<Long, Record>> mapRecords = new ConcurrentHashMap<Long, ConcurrentMap<Long, Record>>(100);
    private final Expression expression;
    private final boolean ordered;
    private final int attributeIndex;
    private volatile TreeSet<Long> valueOrder = null;
    private volatile byte returnType = -1;
    volatile boolean strong = false;
    volatile boolean checkedStrength = false;

    MapIndex(Expression expression, boolean ordered, int attributeIndex) {
        this.expression = expression;
        this.ordered = ordered;
        this.attributeIndex = attributeIndex;
    }

    public void index(long newValue, Record record) {
        if (expression != null && returnType == -1) {
            returnType = record.getIndexTypes()[attributeIndex];
        }
        final long recordId = record.getId();
        Long oldValue = recordValues.get(recordId);
        if (record.isActive()) {
            // add or update
            if (oldValue == null) {
                // record is new
                newRecordIndex(newValue, record);
            } else if (!oldValue.equals(newValue)) {
                // record is updated
                removeRecordIndex(oldValue, recordId);
                newRecordIndex(newValue, record);
            }
        } else {
            // remove the index
            if (oldValue != null) {
                removeRecordIndex(oldValue, recordId);
            }
            recordValues.remove(recordId);
        }
        if (ordered && valueOrder == null) {
//                orderValues();
        }
    }

    public long extractLongValue(Object value) {
        Object extractedValue = expression.getValue(value);
        if (extractedValue == null) {
            return Long.MAX_VALUE;
        } else {
            if (!checkedStrength) {
                if (extractedValue instanceof Boolean || extractedValue instanceof Number) {
                    strong = true;
                }
                checkedStrength = true;
            }
            return QueryService.getLongValue(extractedValue);
        }
    }

    private void newRecordIndex(long newValue, Record record) {
        long recordId = record.getId();
        ConcurrentMap<Long, Record> records = mapRecords.get(newValue);
        if (records == null) {
            records = new ConcurrentHashMap<Long, Record>(100);
            mapRecords.put(newValue, records);
            invalidateOrder();
        }
        records.put(recordId, record);
        recordValues.put(recordId, newValue);
    }

    private void removeRecordIndex(long oldValue, long recordId) {
        recordValues.remove(recordId);
        ConcurrentMap<Long, Record> records = mapRecords.get(oldValue);
        if (records != null) {
            records.remove(recordId);
            if (records.size() == 0) {
                mapRecords.remove(oldValue);
                invalidateOrder();
            }
        }
    }

    private void invalidateOrder() {
        valueOrder = null;
    }

    public Set<MapEntry> getRecords(long value) {
        Set<MapEntry> results = new HashSet<MapEntry>();
        ConcurrentMap<Long, Record> records = mapRecords.get(value);
        if (records != null) {
            results.addAll(records.values());
        }
        return results;
    }

    class SingleResultSet extends AbstractSet<MapEntry> {
        final Collection<? extends MapEntry> records;

        SingleResultSet(Collection<Record> records) {
            this.records = records;
        }

        @Override
        public Iterator<MapEntry> iterator() {
            return (Iterator<MapEntry>) records.iterator();
        }

        @Override
        public int size() {
            return records.size();
        }
    }

    class MultiResultSet extends AbstractSet<MapEntry> {
        private List<Collection<Record>> resultSets = new ArrayList<Collection<Record>>();

        MultiResultSet() {
        }

        public void addResultSet(Collection<Record> resultSet) {
            resultSets.add(resultSet);
        }

        @Override
        public Iterator<MapEntry> iterator() {
            return new It();
        }

        class It implements Iterator<MapEntry> {
            int currentIndex = 0;
            Iterator<Record> currentIterator;

            public boolean hasNext() {
                if (resultSets == null) return false;
                if (currentIterator != null && currentIterator.hasNext()) {
                    return true;
                } else {
                    currentIterator = null;
                }
                while (currentIndex < resultSets.size()) {
                    currentIterator = resultSets.get(currentIndex++).iterator();
                    if (currentIterator.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            public MapEntry next() {
                if (resultSets == null) return null;
                return currentIterator.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (Collection<Record> resultSet : resultSets) {
                size += resultSet.size();
            }
            return size;
        }
    }

    public Set<MapEntry> getRecords(long[] values) {
        Set<MapEntry> results = new HashSet<MapEntry>();
        for (Long value : values) {
            ConcurrentMap<Long, Record> records = mapRecords.get(value);
            if (records != null) {
                results.addAll(records.values());
            }
        }
        return results;
    }

    public Set<MapEntry> getSubRecordsBetween(long from, long to) {
        Set<MapEntry> results = new HashSet<MapEntry>();
        if (valueOrder != null) {
            Set<Long> values = valueOrder.subSet(from, to);
            for (Long value : values) {
                ConcurrentMap<Long, Record> records = mapRecords.get(value);
                if (records != null) {
                    results.addAll(records.values());
                }
            }
            // to wasn't included so include now
            ConcurrentMap<Long, Record> records = mapRecords.get(to);
            if (records != null) {
                results.addAll(records.values());
            }
            return results;
        } else {
            Set<Long> values = mapRecords.keySet();
            for (Long value : values) {
                if (value >= from && value <= to) {
                    ConcurrentMap<Long, Record> records = mapRecords.get(value);
                    if (records != null) {
                        results.addAll(records.values());
                    }
                }
            }
        }
        return results;
    }

    public Set<MapEntry> getSubRecords(boolean equal, boolean lessThan, long searchedValue) {
        Set<MapEntry> results = new HashSet<MapEntry>();
        if (valueOrder != null) {
            Set<Long> values = (lessThan) ? valueOrder.headSet(searchedValue) : valueOrder.tailSet(searchedValue);
            for (Long value : values) {
                // exclude from when you have '>'
                // because from is included
                if (lessThan || equal || !value.equals(searchedValue)) {
                    ConcurrentMap<Long, Record> records = mapRecords.get(value);
                    if (records != null) {
                        results.addAll(records.values());
                    }
                }
            }
            if (lessThan && equal) {
                // to wasn't included so include now
                ConcurrentMap<Long, Record> records = mapRecords.get(searchedValue);
                if (records != null) {
                    results.addAll(records.values());
                }
            }
        } else {
            Set<Long> values = mapRecords.keySet();
            for (Long value : values) {
                boolean valid;
                if (lessThan) {
                    valid = (equal) ? (value <= searchedValue) : (value < searchedValue);
                } else {
                    valid = (equal) ? (value >= searchedValue) : (value > searchedValue);
                }
                if (valid) {
                    ConcurrentMap<Long, Record> records = mapRecords.get(value);
                    if (records != null) {
                        results.addAll(records.values());
                    }
                }
            }
        }
        return results;
    }

    private void orderValues() {
        if (ordered) {
            TreeSet<Long> ordered = new TreeSet<Long>();
            Set<Long> values = mapRecords.keySet();
            for (Long value : values) {
                ordered.add(value);
            }
            valueOrder = ordered;
        }
    }

    public byte getIndexType() {
        if (returnType == -1) {
            Predicates.GetExpressionImpl ex = (Predicates.GetExpressionImpl) expression;
            returnType = getIndexType(ex.getter.getReturnType());
        }
        return returnType;
    }

    public byte getIndexType(Class klass) {
        if (klass == String.class) {
            return 1;
        } else if (klass == int.class || klass == Integer.class) {
            return 2;
        } else if (klass == long.class || klass == Long.class) {
            return 3;
        } else if (klass == boolean.class || klass == Boolean.class) {
            return 4;
        } else if (klass == double.class || klass == Double.class) {
            return 5;
        } else if (klass == float.class || klass == Float.class) {
            return 6;
        } else if (klass == byte.class || klass == Byte.class) {
            return 7;
        } else if (klass == char.class || klass == Character.class) {
            return 8;
        } else {
            return 9;
        }
    }

    long getLongValue(Object value) {
        if (value == null) return Long.MIN_VALUE;
        int valueType = getIndexType(value.getClass());
        if (valueType != returnType) {
            if (value instanceof String) {
                String str = (String) value;
                if (returnType == 2) {
                    value = Integer.valueOf(str);
                } else if (returnType == 3) {
                    value = Long.valueOf(str);
                } else if (returnType == 4) {
                    value = Boolean.valueOf(str);
                } else if (returnType == 5) {
                    value = Double.valueOf(str);
                } else if (returnType == 6) {
                    value = Float.valueOf(str);
                } else if (returnType == 7) {
                    value = Byte.valueOf(str);
                } else if (returnType == 8) {
                    value = Character.valueOf(str.charAt(0));
                }
            }
        }
        return QueryService.getLongValue(value);
    }

    public int getAttributeIndex() {
        return attributeIndex;
    }

    ConcurrentMap<Long, Long> getRecordValues() {
        return recordValues;
    }

    ConcurrentMap<Long, ConcurrentMap<Long, Record>> getMapRecords() {
        return mapRecords;
    }

    public Expression getExpression() {
        return expression;
    }

    public boolean isOrdered() {
        return ordered;
    }

    TreeSet<Long> getValueOrder() {
        return valueOrder;
    }

    public boolean isStrong() {
        return strong;
    }
}
