/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Index {
    // recordId -- indexValue
    private final ConcurrentMap<Long, Long> recordValues = new ConcurrentHashMap<Long, Long>(100, 0.75f, 1);
    // indexValue -- Map<recordId, Record>
    private final IndexStore indexStore;
    private final Expression expression;
    private final boolean ordered;
    private final int attributeIndex;
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
    private static final int TYPE_CHAR = 108;
    private static final int TYPE_DATE = 109;
    private static final int TYPE_UNKNOWN = Byte.MAX_VALUE;

    Index(Expression expression, boolean ordered, int attributeIndex) {
        this.expression = expression;
        this.ordered = ordered;
        this.attributeIndex = attributeIndex;
        if (ordered) {
            indexStore = new SortedIndexStore();
        } else {
            indexStore = new UnsortedIndexStore();
        }
    }

    public void index(Long newValue, Record record) {
        if (expression != null && returnType == -1) {
            setReturnType(record.getIndexTypes()[attributeIndex]);
        }
        final Long recordId = record.getId();
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
    }

    void setReturnType(byte returnType) {
        this.returnType = returnType;
        if (returnType == TYPE_DOUBLE) {
            if (indexStore instanceof SortedIndexStore) {
                throw new RuntimeException("Double type indexes cannot be sorted!");
            }
            UnsortedIndexStore unsortedIndexStore = (UnsortedIndexStore) indexStore;
            unsortedIndexStore.setDoubleValue(true);
        }
    }

    public Long extractLongValue(Object value) {
        Object extractedValue = expression.getValue(value);
        setReturnType(extractedValue);
        if (extractedValue == null) {
            return Long.MIN_VALUE;
        } else {
            setReturnType(getIndexType(extractedValue.getClass()));
            if (!checkedStrength) {
                if (extractedValue instanceof Boolean || extractedValue instanceof Number) {
                    strong = true;
                }
                checkedStrength = true;
            }
            return getLongValueByType(extractedValue);
        }
    }

    private void newRecordIndex(Long newValue, Record record) {
        Long recordId = record.getId();
        indexStore.newRecordIndex(newValue, record);
        recordValues.put(recordId, newValue);
    }

    void removeRecordIndex(Long oldValue, Long recordId) {
        recordValues.remove(recordId);
        indexStore.removeRecordIndex(oldValue, recordId);
    }

    public void appendState(StringBuffer sbState) {
        sbState.append("\nexp:" + expression + ", recordValues:" + recordValues.size() + ", " + indexStore);
    }

    public Set<MapEntry> getRecords(Set<Long> uniqueValues) {
        if (uniqueValues.size() == 1) {
            return indexStore.getRecords(uniqueValues.iterator().next());
        } else {
            MultiResultSet results = new MultiResultSet(recordValues);
            indexStore.getRecords(results, uniqueValues);
            return results;
        }
    }

    public Set<MapEntry> getRecords(Long value) {
        return indexStore.getRecords(value);
    }

    public Set<MapEntry> getSubRecordsBetween(Long from, Long to) {
        MultiResultSet results = new MultiResultSet(recordValues);
        indexStore.getSubRecordsBetween(results, from, to);
        return results;
    }

    public Set<MapEntry> getSubRecords(PredicateType predicateType, Long searchedValue) {
        MultiResultSet results = new MultiResultSet(recordValues);
        indexStore.getSubRecords(results, predicateType, searchedValue);
        return results;
    }

    void setReturnType(Object extractedValue) {
        if (returnType == -1) {
            if (expression instanceof Predicates.GetExpressionImpl) {
                Predicates.GetExpressionImpl ex = (Predicates.GetExpressionImpl) expression;
                setReturnType(getIndexType(ex.getter.getReturnType()));
            } else {
                if (extractedValue == null) throw new RuntimeException("Indexed value cannot be null!");
                setReturnType(getIndexType(extractedValue.getClass()));
            }
        }
    }

    public byte getIndexType() {
        return returnType;
    }

    public static byte getIndexType(Class klass) {
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
            return TYPE_CHAR;
        } else if (Date.class.isAssignableFrom(klass)) { // util.Date, sql.Timestamp, sql.Date
            return TYPE_DATE;
        } else if (klass.isEnum()) {
            return TYPE_STRING;
        } else {
            return TYPE_UNKNOWN;
        }
    }

    private static long getLongValueByType(Object value) {
        if (value == null) return Long.MIN_VALUE;
        if (value instanceof Double) {
            return Double.doubleToLongBits((Double) value);
        } else if (value instanceof Float) {
            return Float.floatToIntBits((Float) value);
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else if (value instanceof String) {
            return getLongValueForString((String) value);
        } else if (value.getClass().isEnum()) {
            return getLongValueForString(value.toString());
        } else if (value instanceof Date) {
            return ((Date) value).getTime();
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
                } else if (returnType == TYPE_CHAR) {
                    value = str.hashCode();
                } else if (returnType == TYPE_DATE) {
                    value = DateHelper.tryParse(str);
                }
            }
        }
        return getLongValueByType(value);
    }

    /**
     * @see String#compareTo(String)
     */
    private static long getLongValueForString(String s) {
        final int maxCharsToIndex = 5;  // just index first 5 chars
        final char[] chars = s.toCharArray();
        long result = 0L;
        for (int i = 0; i < Math.min(maxCharsToIndex, chars.length); i++) {
            result += Math.pow(127, (maxCharsToIndex - i)) * chars[i];
        }
        return result;
    }

    public int getAttributeIndex() {
        return attributeIndex;
    }

    ConcurrentMap<Long, Long> getRecordValues() {
        return recordValues;
    }

    ConcurrentMap<Long, ConcurrentMap<Long, Record>> getMapRecords() {
        return indexStore.getMapRecords();
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
        sb.append("recordValues=").append(recordValues.size());
        sb.append(", ").append(indexStore);
        sb.append(", ordered=").append(ordered);
        sb.append(", strong=").append(strong);
        sb.append(", expression=").append(expression);
        sb.append('}');
        return sb.toString();
    }
}
