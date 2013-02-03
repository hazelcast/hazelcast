/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.impl.Util;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.SerializationHelper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Predicates {

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

        protected boolean doApply(Object first, Object second) {
            final int result = ((Comparable) first).compareTo(second);
            return equal && result == 0 || (less ? (result < 0) : (result > 0));
        }

        public Set<MapEntry> filter(QueryContext queryContext) {
            Index index = queryContext.getMapIndexes().get(first);
            final PredicateType predicateType;
            if (less) {
                predicateType = equal ? PredicateType.LESSER_EQUAL : PredicateType.LESSER;
            } else {
                predicateType = equal ? PredicateType.GREATER_EQUAL : PredicateType.GREATER;
            }
            return index.getSubRecords(predicateType, index.getLongValue(second));
        }

        @Override
        public void readData(DataInput in) throws IOException {
            super.readData(in);
            equal = in.readBoolean();
            less = in.readBoolean();
        }

        @Override
        public void writeData(DataOutput out) throws IOException {
            super.writeData(out);
            out.writeBoolean(equal);
            out.writeBoolean(less);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(first);
            sb.append(less ? "<" : ">");
            if (equal) sb.append("=");
            sb.append(second);
            return sb.toString();
        }
    }

    public static class BetweenPredicate extends EqualPredicate {
        Object to;
        Comparable fromConvertedValue = null;
        Comparable toConvertedValue = null;

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
            if (firstValue == null) {
                return false;
            }
            if (fromConvertedValue == null) {
                fromConvertedValue = (Comparable) getConvertedRealValue(firstValue, second);
                toConvertedValue = (Comparable) getConvertedRealValue(firstValue, to);
            }
            if (firstValue == null || fromConvertedValue == null || toConvertedValue == null) return false;
            return firstValue.compareTo(fromConvertedValue) >= 0 && firstValue.compareTo(toConvertedValue) <= 0;
        }

        public Set<MapEntry> filter(QueryContext queryContext) {
            Index index = queryContext.getMapIndexes().get(first);
            return index.getSubRecordsBetween(index.getLongValue(second), index.getLongValue(to));
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
            return first + " BETWEEN " + second + " AND " + to;
        }
    }

    public static class NotEqualPredicate extends EqualPredicate implements IndexAwarePredicate {
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

        public Set<MapEntry> filter(QueryContext queryContext) {
            Index index = queryContext.getMapIndexes().get(first);
            if (index != null) {
                return index.getSubRecords(PredicateType.NOT_EQUAL, index.getLongValue(second));
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return first + " != " + second;
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
            return "NOT(" + predicate + ")";
        }
    }

    public static class InPredicate extends AbstractPredicate implements IndexAwarePredicate {
        Expression first;
        Object[] inValueArray = null;
        Set inValues = null;
        Set convertedInValues = null;
        Object firstValueObject = null;

        public InPredicate() {
        }

        public InPredicate(Expression first, Object... second) {
            this.first = first;
            this.inValueArray = second;
        }

        private void checkInValues() {
            if (inValues == null) {
                this.inValues = new HashSet(inValueArray.length);
                for (Object o : inValueArray) {
                    inValues.add(o);
                }
            }
        }

        public boolean apply(MapEntry entry) {
            checkInValues();
            if (firstValueObject == null) {
                firstValueObject = inValues.iterator().next();
            }
            Object entryValue = first.getValue(entry);
            if (entryValue == null) return false;
            if (convertedInValues != null) {
                return in(entryValue, convertedInValues);
            } else {
                if (entryValue.getClass() == firstValueObject.getClass()) {
                    return in(entryValue, inValues);
                } else if (firstValueObject instanceof String) {
                    convertedInValues = new HashSet(inValues.size());
                    for (Object objValue : inValues) {
                        convertedInValues.add(getRealObject(entryValue, objValue));
                    }
                    return in(entryValue, convertedInValues);
                }
            }
            return in(entryValue, inValues);
        }

        private static boolean in(Object firstVal, Set values) {
            return values.contains(firstVal);
        }

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index> mapIndexes) {
            if (first instanceof GetExpression) {
                Index index = mapIndexes.get(first);
                if (index != null) {
                    lsIndexPredicates.add(this);
                    return true;
                }
            }
            return false;
        }

        public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index> mapIndexes) {
            Index index = mapIndexes.get(first);
            if (index != null) {
                setAppliedIndexes.add(index);
            }
        }

        public boolean isIndexed(QueryContext queryContext) {
            return queryContext.getMapIndexes().get(first) != null;
        }

        public Set<MapEntry> filter(QueryContext queryContext) {
            checkInValues();
            Index index = queryContext.getMapIndexes().get(first);
            if (index != null) {
                Set<Long> setLongValues = new HashSet<Long>(inValues.size());
                for (Object valueObj : inValues) {
                    final Long value = index.getLongValue(valueObj);
                    setLongValues.add(value);
                }
                return index.getRecords(setLongValues);
            } else {
                return null;
            }
        }

        public Object getValue() {
            return inValues;
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, first);
            out.writeInt(inValueArray.length);
            for (Object value : inValueArray) {
                writeObject(out, value);
            }
        }

        public void readData(DataInput in) throws IOException {
            try {
                first = (Expression) readObject(in);
                int len = in.readInt();
                inValueArray = new Object[len];
                for (int i = 0; i < len; i++) {
                    inValueArray[i] = readObject(in);
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
            for (int i = 0; i < inValueArray.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(inValueArray[i]);
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public static class RegexPredicate extends AbstractPredicate {
        Expression<String> first;
        String regex;
        Pattern pattern = null;

        public RegexPredicate() {
        }

        public RegexPredicate(Expression<String> first, String regex) {
            this.first = first;
            this.regex = regex;
        }

        public boolean apply(MapEntry entry) {
            String firstVal = first.getValue(entry);
            if (firstVal == null) {
                return (regex == null);
            } else if (regex == null) {
                return false;
            } else {
                if (pattern == null) {
                    pattern = Pattern.compile(regex);
                }
                Matcher m = pattern.matcher(firstVal);
                return m.matches();
            }
        }

        public void writeData(DataOutput out) throws IOException {
            writeObject(out, first);
            out.writeUTF(regex);
        }

        public void readData(DataInput in) throws IOException {
            try {
                first = (Expression) readObject(in);
                regex = in.readUTF();
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }

        @Override
        public String toString() {
            return first + " REGEX '" + regex + "'";
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
            this.second = second;
        }

        public boolean apply(MapEntry entry) {
            String firstVal = first.getValue(entry);
            if (firstVal == null) {
                return (second == null);
            } else if (second == null) {
                return false;
            } else {
                if (pattern == null) {
                    pattern = Pattern.compile(second.replaceAll("%", ".*").replaceAll("_", "."));
                }
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
            return first + " LIKE '" + second + "'";
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
                return doApply(first.getValue(entry), ((Expression) second).getValue(entry));
            } else {
                Object firstVal = first.getValue(entry);
                if (firstVal == null) {
                    return (second == null);
                } else if (second == null) {
                    return false;
                } else {
                    if (convertedSecondValue == null) {
                        convertedSecondValue = getConvertedRealValue(firstVal, second);
                    }
                    return doApply(firstVal, convertedSecondValue);
                }
            }
        }

        protected static Object getConvertedRealValue(Object firstValue, Object value) {
            if (firstValue == null) return value;
            if (firstValue.getClass() == value.getClass()) {
                return value;
            } else {
                return getRealObject(firstValue, value);
            }
        }

        protected boolean doApply(Object first, Object second) {
            return first.equals(second);
        }

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index> mapIndexes) {
            if (!secondIsExpression && first instanceof GetExpression) {
                Index index = mapIndexes.get(first);
                if (index != null) {
                    lsIndexPredicates.add(this);
                    return true;
                }
            }
            return false;
        }

        public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index> mapIndexes) {
            Index index = mapIndexes.get(first);
            if (index != null) {
                setAppliedIndexes.add(index);
            }
        }

        public boolean isIndexed(QueryContext queryContext) {
            return queryContext.getMapIndexes().get(first) != null;
        }

        public Set<MapEntry> filter(QueryContext queryContext) {
            Index index = queryContext.getMapIndexes().get(first);
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
            return first + "=" + second;
        }
    }

    public static abstract class AbstractPredicate extends SerializationHelper implements Predicate, DataSerializable {
        public static Object getRealObject(Object type, Object value) {
            String valueString = String.valueOf(value);
            Object result = null;
            if (type instanceof Boolean) {
                result = "true".equalsIgnoreCase(valueString) ? true : false;
            } else if (type instanceof Integer) {
                if (value instanceof Number) {
                    result = ((Number) value).intValue();
                } else {
                    result = Integer.valueOf(valueString);
                }
            } else if (type instanceof Long) {
                if (value instanceof Number) {
                    result = ((Number) value).longValue();
                } else {
                    result = Long.valueOf(valueString);
                }
            } else if (type instanceof Double) {
                if (value instanceof Number) {
                    result = ((Number) value).doubleValue();
                } else {
                    result = Double.valueOf(valueString);
                }
            } else if (type instanceof Float) {
                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    result = Float.valueOf(valueString);
                }
            } else if (type instanceof Byte) {
                if (value instanceof Number) {
                    result = ((Number) value).byteValue();
                } else {
                    result = Byte.valueOf(valueString);
                }
            } else if (type instanceof Timestamp) {
                if (value instanceof Date) { // one of java.util.Date or java.sql.Date
                    result = value;
                } else {
                    result = DateHelper.parseTimeStamp(valueString);
                }
            } else if (type instanceof java.sql.Date) {
                if (value instanceof Date) { // one of java.util.Date or java.sql.Timestamp
                    result = value;
                } else {
                    result = DateHelper.parseSqlDate(valueString);
                }
            } else if (type instanceof Date) {
                if (value instanceof Date) { // one of java.sql.Date or java.sql.Timestamp
                    result = value;
                } else {
                    result = DateHelper.parseDate(valueString);
                }
            } else if (type.getClass().isEnum()) {
                try {
                    String lastEnum = valueString;
                    if (valueString.contains(".")) {
                        // there is a dot  in the value specifier, keep part after last dot
                        lastEnum = valueString.substring(1 + valueString.lastIndexOf("."));
                    }
                    Enum enumType = (Enum) type;
                    result = Enum.valueOf(enumType.getClass(), lastEnum);
                } catch (IllegalArgumentException iae) {
                    // illegal enum value specification
                    throw new IllegalArgumentException("Illegal enum value specification: " + iae.getMessage());
                }
            } else {
                throw new RuntimeException("Unknown type " + type + " value=" + valueString);
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

        public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index> mapIndexes) {
            boolean strong = and;
            if (!mapIndexes.isEmpty()) {
                lsIndexPredicates.add(this);
                if (strong) {
                    final List<IndexAwarePredicate> indexPredicates =
                            new ArrayList<IndexAwarePredicate>(predicates.length);
                    for (Predicate predicate : predicates) {
                        if (predicate instanceof IndexAwarePredicate) {
                            IndexAwarePredicate p = (IndexAwarePredicate) predicate;
                            if (!p.collectIndexAwarePredicates(indexPredicates, mapIndexes)) {
                                strong = false;
                            }
                        } else {
                            strong = false;
                        }
                        if (!strong) {
                            break;
                        }
                    }
                }
                return strong;
            }
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

        public boolean isIndexed(QueryContext queryContext) {
            return true;
        }

        public Set<MapEntry> filter(QueryContext queryContext) {
            Set<MapEntry> results = null;
            for (Predicate predicate : predicates) {
                Set<MapEntry> filter = null;
                if (predicate instanceof IndexAwarePredicate && ((IndexAwarePredicate) predicate).isIndexed(queryContext)) {
                    IndexAwarePredicate p = (IndexAwarePredicate) predicate;
                    filter = p.filter(queryContext);
                } else {
                    filter = new HashSet<MapEntry>();
                    if (and && results != null) {
                        for (MapEntry result : results) {
                            if (predicate.apply(result)) {
                                filter.add(result);
                            }
                        }
                        results = filter;
                        continue;
                    } else {
                        for (MapEntry entry : queryContext.getMapIndexService().getOwnedRecords()) {
                            if (predicate.apply(entry)) {
                                filter.add(entry);
                            }
                        }
                    }
                }
                if (and && (filter == null || filter.isEmpty())) return null;
                if (results == null) {  // first predicate
                    if (and) {
                        results = filter;
                    } else if (filter == null) {
                        results = new HashSet<MapEntry>();
                    } else {
                        results = new HashSet<MapEntry>(filter);
                    }
                } else {
                    if (and) {
                        boolean direct = results.size() < filter.size();
                        final Set<MapEntry> s1 = direct ? results : filter;
                        final Set<MapEntry> s2 = direct ? filter : results;
                        results = new HashSet<MapEntry>();
                        for (MapEntry next : s1) {
                            if (s2.contains(next)) {
                                results.add(next);
                            }
                        }
                        if (results.isEmpty()) return null;
                    } else if (filter != null) { // 'OR' case so add all none-null results
                        results.addAll(filter);
                    }
                }
            }
            return results;
        }

        public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index> mapIndexes) {
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
                if (i > 0) {
                    sb.append(andOr);
                }
                sb.append(predicates[i]);
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
                return " instanceOf (" + klass.getName() + ")";
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

    public static final class GetExpressionImpl<T> extends AbstractExpression implements GetExpression, DataSerializable {
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
                    Getter parent = null;
                    Class clazz = obj.getClass();
                    List<String> possibleMethodNames = new ArrayList<String>(3);
                    for (final String name : input.split("\\.")) {
                        Getter localGetter = null;
                        possibleMethodNames.clear();
                        possibleMethodNames.add(name);
                        final String camelName = Character.toUpperCase(name.charAt(0)) + name.substring(1);
                        possibleMethodNames.add("get" + camelName);
                        possibleMethodNames.add("is" + camelName);
                        if (name.equals("this")) {
                            localGetter = new ThisGetter(parent, obj);
                        } else {
                            for (String methodName : possibleMethodNames) {
                                try {
                                    final Method method = clazz.getMethod(methodName, null);
                                    method.setAccessible(true);
                                    localGetter = new MethodGetter(parent, method);
                                    clazz = method.getReturnType();
                                    break;
                                } catch (NoSuchMethodException ignored) {
                                }
                            }
                            if (localGetter == null) {
                                try {
                                    final Field field = clazz.getField(name);
                                    localGetter = new FieldGetter(parent, field);
                                    clazz = field.getType();
                                } catch (NoSuchFieldException ignored) {
                                }
                            }
                            if (localGetter == null) {
                                Class c = clazz;
                                while (!Object.class.equals(c)) {
                                    try {
                                        final Field field = c.getDeclaredField(name);
                                        field.setAccessible(true);
                                        localGetter = new FieldGetter(parent, field);
                                        clazz = field.getType();
                                        break;
                                    } catch (NoSuchFieldException ignored) {
                                        c = c.getSuperclass();
                                    }
                                }
                            }
                        }
                        if (localGetter == null) {
                            throw new RuntimeException("There is no suitable accessor for '" + name + "'");
                        }
                        parent = localGetter;
                    }
                    getter = parent;
                }
                return getter.getValue(obj);
            } catch (Throwable e) {
                Util.throwUncheckedException(e);
                return null;
            }
        }

        abstract class Getter {
            protected final Getter parent;

            public Getter(final Getter parent) {
                this.parent = parent;
            }

            abstract Object getValue(Object obj) throws Exception;

            abstract Class getReturnType();
        }

        class MethodGetter extends Getter {
            final Method method;

            MethodGetter(Getter parent, Method method) {
                super(parent);
                this.method = method;
            }

            Object getValue(Object obj) throws Exception {
                obj = parent != null ? parent.getValue(obj) : obj;
                return obj != null ? method.invoke(obj) : null;
            }

            Class getReturnType() {
                return this.method.getReturnType();
            }

            @Override
            public String toString() {
                return "MethodGetter [parent=" + parent + ", method=" + method.getName() + "]";
            }
        }

        class FieldGetter extends Getter {
            final Field field;

            FieldGetter(Getter parent, Field field) {
                super(parent);
                this.field = field;
            }

            Object getValue(Object obj) throws Exception {
                obj = parent != null ? parent.getValue(obj) : obj;
                return obj != null ? field.get(obj) : null;
            }

            Class getReturnType() {
                return this.field.getType();
            }

            @Override
            public String toString() {
                return "FieldGetter [parent=" + parent + ", field=" + field + "]";
            }
        }

        class ThisGetter extends Getter {
            final Object object;

            public ThisGetter(final Getter parent, Object object) {
                super(parent);
                this.object = object;
            }

            @Override
            Object getValue(Object obj) throws Exception {
                return obj;
            }

            @Override
            Class getReturnType() {
                return this.object.getClass();
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
            if (!(o instanceof GetExpressionImpl)) return false;
            GetExpressionImpl that = (GetExpressionImpl) o;
            return input.equals(that.input);
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
