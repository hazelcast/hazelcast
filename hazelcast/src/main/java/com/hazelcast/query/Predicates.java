/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            if (equal && result == 0) return true;
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

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(" BETWEEN ");
            sb.append(second);
            sb.append(" AND ");
            sb.append(to);
            return sb.toString();
        }
    }

    public static class NotEqualPredicate extends EqualPredicate {
        public NotEqualPredicate() {
        }

        public NotEqualPredicate(Expression first, Expression second) {
            super(first, second);
        }

        public NotEqualPredicate(Expression first, Object second) {
            super(first, second);
        }

        public boolean apply(MapEntry entry) {
            return !super.apply(entry);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append("!=");
            sb.append(second);
            return sb.toString();
        }
    }

    public static class NotPredicate extends AbstractPredicate {
        Predicate predicate;

        public NotPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public NotPredicate() {
        }

        public boolean apply(MapEntry mapEntry) {
            return !predicate.apply(mapEntry);
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, predicate);
        }

        public void readData(DataInput in) throws IOException {
            predicate = (Predicate) readObject(in);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("NOT(");
            sb.append(predicate.toString());
            sb.append(")");
            return sb.toString();
        }
    }

    public static class InPredicate extends AbstractPredicate implements IndexAwarePredicate {
        Expression first;
        Object[] values = null;
        Object[] convertedValues = null;

        public InPredicate() {
        }

        public InPredicate(Expression first, Object... second) {
            this.first = first;
            this.values = second;
        }

        public boolean apply(MapEntry entry) {
            Object firstVal = first.getValue(entry);
            if (firstVal == null) return false;
            if (convertedValues != null) {
                return in(firstVal, convertedValues);
            } else {
                if (firstVal.getClass() == values[0].getClass()) {
                    return in(firstVal, values);
                } else if (values[0] instanceof String) {
                    convertedValues = new Object[values.length];
                    for (int i = 0; i < values.length; i++) {
                        convertedValues[i] = getRealObject(firstVal.getClass(), (String) values[i]);
                    }
                    return in(firstVal, convertedValues);
                }
            }
            return in(firstVal, values);
        }

        private boolean in(Object firstVal, Object[] values) {
            for (Object o : values) {
                if (firstVal.equals(o)) return true;
            }
            return false;
        }

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index<MapEntry>> mapIndexes) {
            if (first instanceof GetExpression) {
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
                long[] longValues = new long[values.length];
                for (int i = 0; i < values.length; i++) {
                    longValues[i] = index.getLongValue(values[i]);
                }
                return index.getRecords(longValues);
            } else {
                return null;
            }
        }

        public Object getValue() {
            return values;
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, first);
            out.writeInt(values.length);
            for (int i = 0; i < values.length; i++) {
                writeObject(out, values[i]);
            }
        }

        public void readData(DataInput in) throws IOException {
            try {
                first = (Expression) readObject(in);
                int len = in.readInt();
                values = new Object[len];
                for (int i = 0; i < values.length; i++) {
                    values[i] = readObject(in);
                }
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(" IN (");
            for (int i = 0; i < values.length; i++) {
                sb.append(values[i]);
                if (i < (values.length - 1)) {
                    sb.append(",");
                }
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public static class LikePredicate extends AbstractPredicate {
        Expression<String> first;
        String second;
        Pattern pattern = null;

        public LikePredicate() {
        }

        public LikePredicate(Expression<String> first, String second) {
            this.first = first;
            char firstChar = second.charAt(0);
            char lastChar = second.charAt(second.length()-1);
            if (firstChar == '\'' && lastChar != '\'') {
                String replacer = "hz#" + lastChar;
                second = second.substring(0, second.lastIndexOf('\'')+1).replaceAll(replacer, " ");
            }
            this.second = second;
            second = second.replaceAll("%", ".*").replaceAll("_", ".");
            pattern = Pattern.compile(second);
        }

        public boolean apply(MapEntry entry) {
            String firstVal = first.getValue(entry);
            if (firstVal == null) {
                return (second == null);
            } else if (second == null) {
                return false;
            } else {
                Matcher m = pattern.matcher(firstVal);
                return m.matches();
            }
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, first);
            out.writeUTF(second);
        }

        public void readData(DataInput in) throws IOException {
            try {
                first = (Expression) readObject(in);
                second = in.readUTF();
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(" LIKE ");
            sb.append(second);
            return sb.toString();
        }
    }

    public static class EqualPredicate extends AbstractPredicate implements IndexAwarePredicate {
        Expression first;
        Object second;
        Object convertedSecondValue = null;
        protected boolean secondIsExpression = false;

        public EqualPredicate() {
        }

        public EqualPredicate(Expression first, Expression second) {
            this.first = first;
            this.second = second;
            this.secondIsExpression = true;
        }

        public EqualPredicate(Expression first, Object second) {
            this.first = first;
            this.second = second;
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
                        } else if (second instanceof String) {
                            String str = (String) second;
                            convertedSecondValue = getRealObject(firstVal, str);
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
                return index.getRecords(index.getLongValue(second));
            } else {
                return null;
            }
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

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append("=");
            sb.append(second);
            return sb.toString();
        }
    }

    public static abstract class AbstractPredicate extends SerializationHelper implements Predicate, DataSerializable {
        public Object getRealObject(Object type, String value) {
            Object result = null;
            if (type instanceof Boolean) {
                result = "true".equalsIgnoreCase(value) ? true : false;
            } else if (type instanceof Integer) {
                result = Integer.valueOf(value);
            } else if (type instanceof Double) {
                result = Double.valueOf(value);
            } else if (type instanceof Float) {
                result = Float.valueOf(value);
            } else if (type instanceof Byte) {
                result = Byte.valueOf(value);
            } else if (type instanceof Long) {
                result = Long.valueOf(value);
            } else {
                throw new RuntimeException("Unknown type " + type.getClass() + " value=" + value);
            }
            return result;
        }
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

            @Override
            public String toString() {
                final StringBuffer sb = new StringBuffer();
                sb.append(" instanceOf (");
                sb.append(klass.getName());
                sb.append(")");
                return sb.toString();
            }
        };
    }

    public static Predicate and(Predicate x, Predicate y) {
        return new AndOrPredicate(true, x, y);
    }

    public static Predicate not(Predicate predicate) {
        return new NotPredicate(predicate);
    }

    public static Predicate or(Predicate x, Predicate y) {
        return new AndOrPredicate(false, x, y);
    }

    public static Predicate notEqual(final Expression x, final Object y) {
        return new NotEqualPredicate(x, y);
    }

    public static Predicate equal(final Expression x, final Object y) {
        return new EqualPredicate(x, y);
    }

    public static Predicate like(final Expression<String> x, String pattern) {
        return new LikePredicate(x, pattern);
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

    public static <T extends Comparable<T>> Predicate in(Expression<? extends T> expression, T... values) {
        return new InPredicate(expression, values);
    }

    public static Predicate isNot(final Expression<Boolean> x) {
        return new Predicate() {
            public boolean apply(MapEntry entry) {
                Boolean value = x.getValue(entry);
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
        transient Getter getter = null;
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
            return input;
        }
    }
}
