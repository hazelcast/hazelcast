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

import com.hazelcast.core.MapEntry;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Predicates {

    public static class GreaterLessPredicate extends EqualPredicate {
        boolean equal = false;
        boolean less = false;

        public GreaterLessPredicate() {
        }

        public GreaterLessPredicate(Expression first, Expression second, boolean equal, boolean less) {
            super(first, second);
            this.equal = equal;
            this.less = less;
        }

        public GreaterLessPredicate(Expression first, Object second, boolean equal, boolean less) {
            super(first, second);
            this.equal = equal;
            this.less = less;
        }

        public boolean apply(MapEntry entry) {
            int expectedResult = (less) ? -1 : 1;
            Expression<Comparable> cFirst = (Expression<Comparable>) first;
            int result;
            if (secondIsExpression) {
                result = cFirst.getValue(entry).compareTo(((Expression) second).getValue(entry));
            } else {
                result = cFirst.getValue(entry).compareTo(second);
            }
            if (equal && result ==0) return true;
            return (expectedResult == result);
        }

        public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> namedIndexes) {
            Index index = namedIndexes.get(first);
            return index.getSubRecords(equal, less, index.getLongValue(second));
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(less ? "<" : ">");
            sb.append(equal ? "=" : "");
            sb.append(second);
            return sb.toString();
        }
    }

    public static class BetweenPredicate extends EqualPredicate {
        Object to;

        public BetweenPredicate() {
        }

        public BetweenPredicate(Expression first, Expression from, Object to) {
            super(first, from);
            this.to = to;
        }

        public BetweenPredicate(Expression first, Object from, Object to) {
            super(first, from);
            this.to = to;
        }

        public boolean apply(MapEntry entry) {
            Expression<Comparable> cFirst = (Expression<Comparable>) first;
            Comparable firstValue = cFirst.getValue(entry);
            Comparable fromValue = (Comparable) second;
            Comparable toValue = (Comparable) to;
            if (firstValue == null || fromValue == null || toValue == null) return false;
            return firstValue.compareTo(fromValue) >= 0 && firstValue.compareTo(toValue) <= 0;
        }

        public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> namedIndexes) {
            Index index = namedIndexes.get(first);
            return index.getSubRecords(index.getLongValue(second), index.getLongValue(to));
        }

        public void writeData(DataOutput out) throws IOException {
            super.writeData(out);
            writeObject(out, to);
        }

        public void readData(DataInput in) throws IOException {
            super.readData(in);
            to = readObject(in);
        }
    }


    public static class EqualPredicate extends AbstractPredicate implements IndexAwarePredicate {
        Expression first;
        Object second;
        Object convertedSecondValue = null;
        protected boolean secondIsExpression = true;

        public EqualPredicate() {
        }

        public EqualPredicate(Expression first, Expression second) {
            this.first = first;
            this.second = second;
        }

        public EqualPredicate(Expression first, Object second) {
            this.first = first;
            this.second = second;
            this.secondIsExpression = false;
        }

        public boolean apply(MapEntry entry) {
            if (secondIsExpression) {
                return first.getValue(entry).equals(((Expression) second).getValue(entry));
            } else {
                Object firstVal = first.getValue(entry);
                if (firstVal == null) {
                    return (second == null);
                } else if (second == null) {
                    return false;
                } else {
                    if (convertedSecondValue != null) {
                        return firstVal.equals(convertedSecondValue);
                    } else {
                        if (firstVal.getClass() == second.getClass()) {
                           convertedSecondValue = second;
                        } else if (second instanceof String){
                           String str = (String) second;
                           if (firstVal instanceof Boolean) {
                               convertedSecondValue = "true".equalsIgnoreCase(str) ? true : false;
                           } else if (firstVal instanceof Integer) {
                               convertedSecondValue = Integer.valueOf(str);
                           } else if (firstVal instanceof Double) {
                               convertedSecondValue = Double.valueOf(str);
                           } else if (firstVal instanceof Float) {
                               convertedSecondValue = Float.valueOf(str);
                           }  else if (firstVal instanceof Byte) {
                               convertedSecondValue = Byte.valueOf(str);
                           }  else if (firstVal instanceof Long) {
                               convertedSecondValue = Long.valueOf(str);
                           } else {
                               throw new RuntimeException("Unknown type " + firstVal.getClass() + " value=" + str);
                           }
                        }
                    }
                    return firstVal.equals(convertedSecondValue);
                }
            }
        }

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index<MapEntry>> mapIndexes) {
            if (!secondIsExpression && first instanceof GetExpression) {
                Index index = mapIndexes.get(first);
                if (index != null) {
                    lsIndexPredicates.add(this);
                } else {
                    return false;
                }
            }
            return true;
        }

        public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index<MapEntry>> mapIndexes) {
            Index index = mapIndexes.get(first);
            if (index != null) {
                setAppliedIndexes.add(index);
            }
        }

        public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> mapIndexes) {
            Index index = mapIndexes.get(first);
            if (index != null) {
                return index.getRecords (index.getLongValue(second));
            } else {
                return null;
            }
        }

        public boolean isRanged() {
            return false;
        }

        public Object getValue() {
            return second;
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, first);
            out.writeBoolean(secondIsExpression);
            writeObject(out, second);
        }

        public void readData(DataInput in) throws IOException {
            try {
                first = (Expression) readObject(in);
                secondIsExpression = in.readBoolean();
                second = readObject(in);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }

        public Expression getFirst() {
            return first;
        }

        public Object getSecond() {
            return second;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(" = ");
            sb.append(second);
            return sb.toString();
        }
    }

    public static abstract class AbstractPredicate extends SerializationHelper implements Predicate, DataSerializable {

    }

    public static class AndOrPredicate extends AbstractPredicate implements IndexAwarePredicate {
        Predicate[] predicates;
        boolean and = false;

        public AndOrPredicate() {
        }

        public AndOrPredicate(boolean and, Predicate first, Predicate second) {
            this.and = and;
            predicates = new Predicate[]{first, second};
        }


        public AndOrPredicate(boolean and, Predicate... predicates) {
            this.and = and;
            this.predicates = predicates;
        }

        public boolean apply(MapEntry mapEntry) {
            for (Predicate predicate : predicates) {
                boolean result = predicate.apply(mapEntry);
                if (and && !result) return false;
                else if (!and && result) return true;
            }
            return and;
        }

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index<MapEntry>> mapIndexes) {
            boolean strong = and;
            if (and) {
                for (Predicate predicate : predicates) {
                    if (predicate instanceof IndexAwarePredicate) {
                        IndexAwarePredicate p = (IndexAwarePredicate) predicate;
                        if (!p.collectIndexAwarePredicates(lsIndexPredicates, mapIndexes)) {
                            strong = false;
                        }
                    } else {
                        strong = false;
                    }
                }
            }
            return strong;
        }

        public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> mapIndexes) {
            return null;
        }

        public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index<MapEntry>> mapIndexes) {
            if (and) {
                for (Predicate predicate : predicates) {
                    if (predicate instanceof IndexAwarePredicate) {
                        IndexAwarePredicate p = (IndexAwarePredicate) predicate;
                        p.collectAppliedIndexes(setAppliedIndexes, mapIndexes);
                    }
                }
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeBoolean(and);
            out.writeInt(predicates.length);
            for (Predicate predicate : predicates) {
                writeObject(out, predicate);
            }
        }

        public void readData(DataInput in) throws IOException {
            and = in.readBoolean();
            int len = in.readInt();
            predicates = new Predicate[len];
            for (int i = 0; i < len; i++) {
                predicates[i] = (Predicate) readObject(in);
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("(");
            String andOr = (and) ? " AND " : " OR ";
            int size = predicates.length;
            for (int i = 0; i < size; i++) {
                sb.append(predicates[i]);
                if (i < size - 1) {
                    sb.append(andOr);
                }
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public static Predicate instanceOf(final Class klass) {
        return new Predicate() {
            public boolean apply(MapEntry mapEntry) {
                Object value = mapEntry.getValue();
                if (value == null) return false;
                return klass.isAssignableFrom(value.getClass());
            }
        };
    }

    public static Predicate and(Predicate x, Predicate y) {
        return new AndOrPredicate(true, x, y);
    }


    public static Predicate or(Predicate x, Predicate y) {
        return new AndOrPredicate(false, x, y);
    }

    public static Predicate equal(final Expression x, final Object y) {
        return new EqualPredicate(x, y);
    }

    public static <T extends Comparable<T>> Predicate greaterThan(Expression<? extends T> x, T y) {
        return new GreaterLessPredicate(x, y, false, false);
    }

    public static <T extends Comparable<T>> Predicate greaterEqual(Expression<? extends T> x, T y) {
        return new GreaterLessPredicate(x, y, true, false);
    }

    public static <T extends Comparable<T>> Predicate lessThan(Expression<? extends T> x, T y) {
        return new GreaterLessPredicate(x, y, false, true);
    }

    public static <T extends Comparable<T>> Predicate lessEqual(Expression<? extends T> x, T y) {
        return new GreaterLessPredicate(x, y, true, true);
    }

    public static <T extends Comparable<T>> Predicate between(Expression<? extends T> expression, T from, T to) {
        return new BetweenPredicate(expression, from, to);
    }

    public static Predicate not(final Expression<Boolean> x) {
        return new Predicate() {
            public boolean apply(MapEntry entry) {
                Boolean value = x.getValue(entry);
                return Boolean.FALSE.equals(value);
            }
        };
    }

    public static Predicate not(final boolean value) {
        return new Predicate() {
            public boolean apply(MapEntry entry) {
                return Boolean.FALSE.equals(value);
            }
        };
    }

    public static GetExpression get(final String methodName) {
        return new GetExpressionImpl(methodName);
    }


    public static abstract class AbstractExpression extends SerializationHelper implements Expression {

    }

    interface GetExpression<T> extends Expression {
        GetExpression get(String fieldName);
    }

    public static class GetExpressionImpl<T> extends AbstractExpression implements GetExpression, DataSerializable {
        Getter getter = null;
        String input;
        List<GetExpressionImpl<T>> ls = null;

        public GetExpressionImpl() {
        }

        public GetExpressionImpl(String input) {
            this.input = input;
        }

        public GetExpression get(String methodName) {
            if (ls == null) {
                ls = new ArrayList();
            }
            ls.add(new GetExpressionImpl(methodName));
            return this;
        }

        public Object getValue(Object obj) {
            if (ls != null) {
                Object result = doGetValue(obj);
                for (GetExpressionImpl<T> e : ls) {
                    result = e.doGetValue(result);
                }
                return result;
            } else {
                return doGetValue(obj);
            }
        }

        private Object doGetValue(Object obj) {
            if (obj instanceof MapEntry) {
                obj = ((MapEntry) obj).getValue();
            }
            if (obj == null) return null;
            try {
                if (getter == null) {
                    List<String> possibleMethodNames = new ArrayList<String>(3);
                    possibleMethodNames.add(input);
                    possibleMethodNames.add("get" + input.substring(0, 1).toUpperCase() + input.substring(1));
                    possibleMethodNames.add("is" + input.substring(0, 1).toUpperCase() + input.substring(1));
                    getter:
                    for (String methodName : possibleMethodNames) {
                        try {
                            getter = new MethodGetter(obj.getClass().getMethod(methodName, null));
                            break getter;
                        } catch (NoSuchMethodException ignored) {
                        }
                    }
                    if (getter == null) {
                        try {
                            getter = new FieldGetter(obj.getClass().getField(input));
                        } catch (NoSuchFieldException ignored) {
                        }
                    }

                    if (getter == null) {
                        throw new RuntimeException("There is no method of field matching " + input);
                    }
                }
                return getter.getValue(obj);
            } catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        abstract class Getter {
            abstract Object getValue(Object obj) throws Exception;

            abstract Class getReturnType();
        }

        class MethodGetter extends Getter {
            final Method method;

            MethodGetter(Method method) {
                this.method = method;
            }

            Object getValue(Object obj) throws Exception {
                return method.invoke(obj);
            }

            Class getReturnType() {
                return this.method.getReturnType();
            }
        }

        class FieldGetter extends Getter {
            final Field field;

            FieldGetter(Field field) {
                this.field = field;
            }

            Object getValue(Object obj) throws Exception {
                return field.get(obj);
            }

            Class getReturnType() {
                return this.field.getType();
            }
        }


        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(input);
        }

        public void readData(DataInput in) throws IOException {
            input = in.readUTF();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GetExpressionImpl that = (GetExpressionImpl) o;

            if (!input.equals(that.input)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return input.hashCode();
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("get('" + input + "')");
            return sb.toString();
        }
    }

}
