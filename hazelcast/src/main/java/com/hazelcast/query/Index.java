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
    volatile byte returnType = -1;

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

    void addNewIndex(long value, byte type, T record) {
        if (returnType == -1) {
            returnType = type;
        } else if (returnType != type) {
            throw new RuntimeException("Index types are not the same. Found:" + returnType + " New:" + type);
        }
        Set<T> lsRecords = mapIndex.get(value);
        if (lsRecords == null) {
            lsRecords = new LinkedHashSet<T>();
            mapIndex.put(value, lsRecords);
        }
        lsRecords.add(record);
    }

    void updateIndex(long oldValue, long newValue, byte type, T record) {
        if (oldValue == Long.MIN_VALUE) {
            addNewIndex(newValue, type, record);
        } else {
            if (oldValue != newValue) {
                removeIndex(oldValue, record);
                addNewIndex(newValue, type, record);
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
                }else if (returnType == 7) {
                    value = Byte.valueOf(str);
                }else if (returnType == 8) {
                    value = Character.valueOf(str.charAt(0));
                }
            }
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean.TRUE.equals(value)) ? 1 : -1;
        } else {
            return value.hashCode();
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

        return expression.equals(index.expression);

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