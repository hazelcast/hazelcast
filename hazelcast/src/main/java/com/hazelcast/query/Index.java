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

public class Index {
    // recordId -- indexValue
    private final ConcurrentMap<Long, Long> recordValues = new ConcurrentHashMap<Long, Long>(10000, 0.75f, 1);
    // indexValue -- Map<recordId, Record>
    private final ConcurrentMap<Long, ConcurrentMap<Long, Record>> mapRecords = new ConcurrentHashMap<Long, ConcurrentMap<Long, Record>>(100, 0.75f, 1);
    private final Expression expression;
    private final boolean ordered;
    private final int attributeIndex;
    private volatile TreeSet<Long> valueOrder = null;
    private volatile byte returnType = -1;
    volatile boolean strong = false;
    volatile boolean checkedStrength = false;

    private static final int TYPE_STRING = 101;
    private static final int TYPE_INT = 102;
    private static final int TYPE_LONG = 103;
    private static final int TYPE_BOOLEAN = 104;
    private static final int TYPE_DOUBLE = 105;
    private static final int TYPE_FLOAT = 106;
    private static final int TYPE_BYTE = 107;
    private static final int TYPE_CLASS = 108;
    private static final int TYPE_UNKNOWN = 109;

    Index(Expression expression, boolean ordered, int attributeIndex) {
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
            return getLongValueByType(extractedValue);
        }
    }

    private void newRecordIndex(long newValue, Record record) {
        long recordId = record.getId();
        ConcurrentMap<Long, Record> records = mapRecords.get(newValue);
        if (records == null) {
            records = new ConcurrentHashMap<Long, Record>(1, 0.75f, 1);
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

    public void appendState(StringBuffer sbState) {
        sbState.append("\nexp:" + expression + ", recordValues:" + recordValues.size() + ", valueRecords: " + mapRecords.size());
    }

    class SingleResultSet extends AbstractSet<MapEntry> {
        private final ConcurrentMap<Long, ? extends MapEntry> records;

        public SingleResultSet(ConcurrentMap<Long, Record> records) {
            this.records = records;
        }

        @Override
        public boolean contains(Object mapEntry) {
            return records != null && records.containsKey(((Record) mapEntry).getId());
        }

        @Override
        public Iterator<MapEntry> iterator() {
            if (records == null) {
                return new HashSet<MapEntry>().iterator();
            } else {
                return (Iterator<MapEntry>) records.values().iterator();
            }
        }

        @Override
        public int size() {
            return (records == null) ? 0 : records.size();
        }
    }

    class MultiResultSet extends AbstractSet<MapEntry> {
        private List<Collection<Record>> resultSets = new ArrayList<Collection<Record>>();
        private Set<Long> indexValues = new HashSet<Long>();

        MultiResultSet() {
        }

        public void addResultSet(Long indexValue, Collection<Record> resultSet) {
            resultSets.add(resultSet);
            indexValues.add(indexValue);
        }

        @Override
        public Iterator<MapEntry> iterator() {
            return new It();
        }

        @Override
        public boolean contains(Object mapEntry) {
            Long indexValue = recordValues.get(((Record) mapEntry).getId());
            if (indexValue == null) return false;
            else return indexValues.contains(indexValue);
        }

        class It implements Iterator<MapEntry> {
            int currentIndex = 0;
            Iterator<Record> currentIterator;

            public boolean hasNext() {
//                if (true) throw new RuntimeException();
                if (resultSets.size() == 0) return false;
                if (currentIterator != null && currentIterator.hasNext()) {
                    return true;
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
                if (resultSets.size() == 0) return null;
                return currentIterator.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public boolean add(MapEntry obj) {
            throw new UnsupportedOperationException();
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
        MultiResultSet results = new MultiResultSet();
        for (Long value : values) {
            ConcurrentMap<Long, Record> records = mapRecords.get(value);
            if (records != null) {
                results.addResultSet(value, records.values());
            }
        }
        return results;
    }

    public Set<MapEntry> getSubRecordsBetween(long from, long to) {
        MultiResultSet results = new MultiResultSet();
        if (valueOrder != null) {
            Set<Long> values = valueOrder.subSet(from, to);
            for (Long value : values) {
                ConcurrentMap<Long, Record> records = mapRecords.get(value);
                if (records != null) {
                    results.addResultSet(value, records.values());
                }
            }
            // to wasn't included so include now
            ConcurrentMap<Long, Record> records = mapRecords.get(to);
            if (records != null) {
                results.addResultSet(to, records.values());
            }
            return results;
        } else {
            Set<Long> values = mapRecords.keySet();
            for (Long value : values) {
                if (value >= from && value <= to) {
                    ConcurrentMap<Long, Record> records = mapRecords.get(value);
                    if (records != null) {
                        results.addResultSet(value, records.values());
                    }
                }
            }
        }
        return results;
    }

    public Set<MapEntry> getRecords(long value) {
        return new SingleResultSet(mapRecords.get(value));
    }

    public Set<MapEntry> getSubRecords(boolean equal, boolean lessThan, long searchedValue) {
        MultiResultSet results = new MultiResultSet();
        if (valueOrder != null) {
            Set<Long> values = (lessThan) ? valueOrder.headSet(searchedValue) : valueOrder.tailSet(searchedValue);
            for (Long value : values) {
                // exclude from when you have '>'
                // because from is included
                if (lessThan || equal || !value.equals(searchedValue)) {
                    ConcurrentMap<Long, Record> records = mapRecords.get(value);
                    if (records != null) {
                        results.addResultSet(value, records.values());
                    }
                }
            }
            if (lessThan && equal) {
                // to wasn't included so include now
                ConcurrentMap<Long, Record> records = mapRecords.get(searchedValue);
                if (records != null) {
                    results.addResultSet(searchedValue, records.values());
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
                        results.addResultSet(value, records.values());
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
            return TYPE_STRING;
        } else if (klass == int.class || klass == Integer.class) {
            return TYPE_INT;
        } else if (klass == long.class || klass == Long.class) {
            return TYPE_LONG;
        } else if (klass == boolean.class || klass == Boolean.class) {
            return TYPE_BOOLEAN;
        } else if (klass == double.class || klass == Double.class) {
            return TYPE_DOUBLE;
        } else if (klass == float.class || klass == Float.class) {
            return TYPE_FLOAT;
        } else if (klass == byte.class || klass == Byte.class) {
            return TYPE_BYTE;
        } else if (klass == char.class || klass == Character.class) {
            return TYPE_CLASS;
        } else {
            return TYPE_UNKNOWN;
        }
    }

    private static long getLongValueByType(Object value) {
        if (value == null) return Long.MAX_VALUE;
        if (value instanceof Double) {
            return Double.doubleToLongBits((Double) value);
        } else if (value instanceof Float) {
            return Float.floatToIntBits((Float) value);
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else if (value instanceof String) {
            String str = (String) value;
            if (str.length() == 0) {
                return 0;
            } else {
                return str.charAt(0);
            }
        } else {
            return value.hashCode();
        }
    }

    long getLongValue(Object value) {
        if (value == null) return Long.MIN_VALUE;
        int valueType = getIndexType(value.getClass());
        if (valueType != returnType) {
            if (value instanceof String) {
                String str = (String) value;
                if (returnType == TYPE_INT) {
                    value = Integer.valueOf(str);
                } else if (returnType == TYPE_LONG) {
                    value = Long.valueOf(str);
                } else if (returnType == TYPE_BOOLEAN) {
                    value = Boolean.valueOf(str);
                } else if (returnType == TYPE_DOUBLE) {
                    value = Double.valueOf(str);
                } else if (returnType == TYPE_FLOAT) {
                    value = Float.valueOf(str);
                } else if (returnType == TYPE_BYTE) {
                    value = Byte.valueOf(str);
                } else if (returnType == TYPE_CLASS) {
                    value = Character.valueOf(str.charAt(0));
                }
            }
        }
        return getLongValueByType(value);
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

    public boolean isStrong() {
        return strong;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return expression.equals(index.expression);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("Index{");
        sb.append("indexValues=").append(mapRecords.size());
        sb.append(", recordValues=").append(recordValues.size());
        sb.append(", ordered=").append(ordered);
        sb.append(", strong=").append(strong);
        sb.append(", expression=").append(expression);
        sb.append('}');
        return sb.toString();
    }
}
