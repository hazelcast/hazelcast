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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Predicates {

    public static class BetweenPredicate extends AbstractPredicate {
        Comparable to = null;
        Comparable from = null;

        public BetweenPredicate() {
        }

        public BetweenPredicate(String first, Comparable from, Comparable to) {
            super(first);
            this.from = from;
            this.to = to;
        }

        public boolean apply(Map.Entry entry) {
            Comparable firstValue = readAttribute(entry);
            if (firstValue == null) {
                return false;
            }
            Comparable fromConvertedValue = convert(entry, from);
            Comparable toConvertedValue = convert(entry, to);
            if (fromConvertedValue == null || toConvertedValue == null) {
                return false;
            }
            return firstValue.compareTo(fromConvertedValue) >= 0 && firstValue.compareTo(toConvertedValue) <= 0;
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            return index.getSubRecordsBetween(from, to);
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeObject(to);
        }

        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            to = in.readObject();
        }

        @Override
        public String toString() {
            return attribute + " BETWEEN " + from + " AND " + to;
        }
    }

    public static class NotPredicate implements Predicate, DataSerializable {
        Predicate predicate;

        public NotPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public NotPredicate() {
        }

        public boolean apply(Map.Entry mapEntry) {
            return !predicate.apply(mapEntry);
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(predicate);
        }

        public void readData(ObjectDataInput in) throws IOException {
            predicate = (Predicate) in.readObject();
        }

        @Override
        public String toString() {
            return "NOT(" + predicate + ")";
        }
    }

    public static class InPredicate extends AbstractPredicate {
        Comparable[] values = null;
        Set convertedInValues = null;

        public InPredicate() {
        }

        public InPredicate(String attribute, Comparable... values) {
            super(attribute);
            this.values = values;
        }

        private void checkInValues(Map.Entry entry) {
            if (convertedInValues == null) {
                convertedInValues = new HashSet(values.length);
                for (Comparable value : values) {
                    convertedInValues.add(convert(entry, value));
                }
            }
        }

        public boolean apply(Map.Entry entry) {
            checkInValues(entry);
            Comparable entryValue = readAttribute(entry);
            return entryValue != null && in(entryValue, convertedInValues);
        }

        private static boolean in(Object firstVal, Set values) {
            return values.contains(firstVal);
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            if (index != null) {
                return index.getRecords(values);
            } else {
                return null;
            }
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeInt(values.length);
            for (Object value : values) {
                out.writeObject(value);
            }
        }

        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            int len = in.readInt();
            values = new Comparable[len];
            for (int i = 0; i < len; i++) {
                values[i] = in.readObject();
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(attribute);
            sb.append(" IN (");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(values[i]);
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public static class RegexPredicate implements Predicate, DataSerializable {
        String attribute;
        String regex;
        Pattern pattern = null;

        public RegexPredicate() {
        }

        public RegexPredicate(String attribute, String regex) {
            this.attribute = attribute;
            this.regex = regex;
        }

        public boolean apply(Map.Entry entry) {
            String firstVal = (String) readAttribute(entry, attribute);
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

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
            out.writeUTF(regex);
        }

        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
            regex = in.readUTF();
        }

        @Override
        public String toString() {
            return attribute + " REGEX '" + regex + "'";
        }
    }

    public static class LikePredicate implements Predicate, DataSerializable {
        String attribute;
        String second;
        Pattern pattern = null;

        public LikePredicate() {
        }

        public LikePredicate(String attribute, String second) {
            this.attribute = attribute;
            this.second = second;
        }

        public boolean apply(Map.Entry entry) {
            String firstVal = (String) readAttribute(entry, attribute);
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

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
            out.writeUTF(second);
        }

        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
            second = in.readUTF();
        }

        @Override
        public String toString() {
            return attribute + " LIKE '" + second + "'";
        }
    }

    public static class AndPredicate implements IndexAwarePredicate, DataSerializable {

        protected Predicate[] predicates;

        public AndPredicate() {
        }

        public AndPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Set<QueryableEntry> smallestIndexedResult = null;
            List<Set<QueryableEntry>> otherIndexedResults = new LinkedList<Set<QueryableEntry>>();
            List<Predicate> lsNoIndexPredicates = null;
            for (Predicate predicate : predicates) {
                boolean indexed = false;
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (iap.isIndexed(queryContext)) {
                        indexed = true;
                        Set<QueryableEntry> s = iap.filter(queryContext);
                        if (smallestIndexedResult == null) {
                            smallestIndexedResult = s;
                        } else if (s.size() < smallestIndexedResult.size()) {
                            otherIndexedResults.add(smallestIndexedResult);
                            smallestIndexedResult = s;
                        } else {
                            otherIndexedResults.add(s);
                        }
                    } else {
                        if (lsNoIndexPredicates == null) {
                            lsNoIndexPredicates = new LinkedList<Predicate>();
                            lsNoIndexPredicates.add(predicate);
                        }
                    }
                }
                if (!indexed) {
                    if (lsNoIndexPredicates == null) {
                        lsNoIndexPredicates = new LinkedList<Predicate>();
                    }
                    lsNoIndexPredicates.add(predicate);
                }
            }
            if (smallestIndexedResult == null) {
                return null;
            }
            return new AndResultSet(smallestIndexedResult, otherIndexedResults, lsNoIndexPredicates);
        }

        public boolean isIndexed(QueryContext queryContext) {
            for (Predicate predicate : predicates) {
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (iap.isIndexed(queryContext)) {
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean apply(Map.Entry mapEntry) {
            for (Predicate predicate : predicates) {
                if (!predicate.apply(mapEntry)) return false;
            }
            return true;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("(");
            int size = predicates.length;
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append(predicates[i]);
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(predicates.length);
            for (int i = 0; i < predicates.length; i++) {
                out.writeObject(predicates[i]);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            predicates = new Predicate[size];
            for (int i = 0; i < size; i++) {
                predicates[i] = in.readObject();
            }
        }
    }

    public static class OrPredicate implements IndexAwarePredicate, DataSerializable {

        private Predicate[] predicates;

        public OrPredicate() {
        }

        public OrPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            List<Set<QueryableEntry>> indexedResults = new LinkedList<Set<QueryableEntry>>();
            for (Predicate predicate : predicates) {
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (iap.isIndexed(queryContext)) {
                        Set<QueryableEntry> s = iap.filter((QueryContext) filter(queryContext));
                        indexedResults.add(s);
                    } else {
                        return null;
                    }
                }
            }
            return new OrResultSet(indexedResults);
        }

        public boolean isIndexed(QueryContext queryContext) {
            for (Predicate predicate : predicates) {
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (!iap.isIndexed(queryContext)) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean apply(Map.Entry mapEntry) {
            for (Predicate predicate : predicates) {
                if (predicate.apply(mapEntry)) return true;
            }
            return false;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("(");
            int size = predicates.length;
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append(predicates[i]);
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(predicates.length);
            for (int i = 0; i < predicates.length; i++) {
                out.writeObject(predicates[i]);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            predicates = new Predicate[size];
            for (int i = 0; i < size; i++) {
                predicates[i] = in.readObject();
            }
        }
    }

    public static class GreaterLessPredicate extends EqualPredicate {
        boolean equal = false;
        boolean less = false;

        public GreaterLessPredicate() {
        }

        public GreaterLessPredicate(String attribute, Comparable value, boolean equal, boolean less) {
            super(attribute, value);
            this.equal = equal;
            this.less = less;
        }

        public boolean apply(Map.Entry mapEntry) {
            final int result = readAttribute(mapEntry).compareTo(convert(mapEntry, value));
            return equal && result == 0 || (less ? (result < 0) : (result > 0));
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            final ComparisonType comparisonType;
            if (less) {
                comparisonType = equal ? ComparisonType.LESSER_EQUAL : ComparisonType.LESSER;
            } else {
                comparisonType = equal ? ComparisonType.GREATER_EQUAL : ComparisonType.GREATER;
            }
            return index.getSubRecords(comparisonType, value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            equal = in.readBoolean();
            less = in.readBoolean();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeBoolean(equal);
            out.writeBoolean(less);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(attribute);
            sb.append(less ? "<" : ">");
            if (equal) sb.append("=");
            sb.append(value);
            return sb.toString();
        }
    }

    public static class NotEqualPredicate extends EqualPredicate {
        public NotEqualPredicate() {
        }

        public NotEqualPredicate(String attribute, Comparable value) {
            super(attribute, value);
        }

        public boolean apply(Map.Entry entry) {
            return !super.apply(entry);
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            if (index != null) {
                return index.getSubRecords(ComparisonType.NOT_EQUAL, value);
            } else {
                return null;
            }
        }

        @Override
        public String toString() {
            return attribute + " != " + value;
        }
    }

    public static class EqualPredicate extends AbstractPredicate {
        protected Comparable value;

        public EqualPredicate() {
        }

        public EqualPredicate(String attribute, Comparable value) {
            super(attribute);
            this.value = value;
        }

        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            return index.getRecords(value);
        }

        public boolean apply(Map.Entry mapEntry) {
            Comparable entryValue = readAttribute(mapEntry);
            if (entryValue == null) {
                return value == null || value == IndexImpl.NULL;
            }
            value = convert(mapEntry, value);
            return value != null && value.equals(entryValue);
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeObject(value);
        }

        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            value = in.readObject();
        }

        @Override
        public String toString() {
            return attribute + "=" + value;
        }
    }

    public static abstract class AbstractPredicate implements IndexAwarePredicate, DataSerializable {

        protected String attribute;
        private AttributeType attributeType = null;

        protected AbstractPredicate() {
        }

        protected AbstractPredicate(String attribute) {
            this.attribute = attribute;
        }

        protected Comparable convert(Map.Entry mapEntry, Comparable comparable) {
            if (comparable == null) return null;
            if (attributeType == null) {
                QueryableEntry queryableEntry = (QueryableEntry) mapEntry;
                attributeType = queryableEntry.getAttributeType(attribute);
            }
            if (attributeType != null) {
                return attributeType.getConverter().convert(comparable);
            }
            return comparable;
        }

        public boolean isIndexed(QueryContext queryContext) {
            return getIndex(queryContext) != null;
        }

        protected Index getIndex(QueryContext queryContext) {
            return queryContext.getIndex(attribute);
        }

        protected Comparable readAttribute(Map.Entry entry) {
            QueryableEntry queryableEntry = (QueryableEntry) entry;
            Comparable attValue = queryableEntry.getAttribute(attribute);
            return convert(entry, attValue);
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
        }

        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
        }
    }

    private static Comparable readAttribute(Map.Entry entry, String attribute) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Comparable value = queryableEntry.getAttribute(attribute);
        if (value == null) return IndexImpl.NULL;
        AttributeType attributeType = queryableEntry.getAttributeType(attribute);
        return attributeType.getConverter().convert(value);
    }

    public static Predicate and(Predicate x, Predicate y) {
        return new AndPredicate(x, y);
    }

    public static Predicate not(Predicate predicate) {
        return new NotPredicate(predicate);
    }

    public static Predicate or(Predicate x, Predicate y) {
        return new OrPredicate(x, y);
    }

    public static Predicate notEqual(String attribute, Comparable y) {
        return new NotEqualPredicate(attribute, y);
    }

    public static Predicate equal(String attribute, Comparable y) {
        return new EqualPredicate(attribute, y);
    }

    public static Predicate like(String attribute, String pattern) {
        return new LikePredicate(attribute, pattern);
    }

    public static Predicate greaterThan(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, false, false);
    }

    public static Predicate greaterEqual(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, true, false);
    }

    public static Predicate lessThan(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, false, true);
    }

    public static Predicate lessEqual(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, true, true);
    }

    public static Predicate between(String attribute, Comparable from, Comparable to) {
        return new BetweenPredicate(attribute, from, to);
    }

    public static Predicate in(String attribute, Comparable... values) {
        return new InPredicate(attribute, values);
    }
}
