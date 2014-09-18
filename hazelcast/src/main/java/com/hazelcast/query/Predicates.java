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
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.ComparisonType;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.OrResultSet;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class to create {@link com.hazelcast.query.Predicate} instances.
 */
public final class Predicates {

    //we don't want instances. private constructor.
    private Predicates() {
    }

    public static Predicate instanceOf(final Class klass) {
        return new InstanceOfPredicate(klass);
    }

    private static Comparable readAttribute(Map.Entry entry, String attribute) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Comparable value = queryableEntry.getAttribute(attribute);
        if (value == null) {
            return IndexImpl.NULL;
        }
        return value;
    }

    public static Predicate and(Predicate... predicates) {
        return new AndPredicate(predicates);
    }

    public static Predicate not(Predicate predicate) {
        return new NotPredicate(predicate);
    }

    /**
     * Or predicate
     *
     * @param predicates
     * @return
     */
    public static Predicate or(Predicate... predicates) {
        return new OrPredicate(predicates);
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

    public static Predicate ilike(String attribute, String pattern) {
        return new ILikePredicate(attribute, pattern);
    }

    public static Predicate regex(String attribute, String pattern) {
        return new RegexPredicate(attribute, pattern);
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

    /**
     * Between Predicate
     */
    public static class BetweenPredicate extends AbstractPredicate {
        private Comparable to;
        private Comparable from;

        public BetweenPredicate() {
        }

        public BetweenPredicate(String first, Comparable from, Comparable to) {
            super(first);
            if (from == null || to == null) {
                throw new NullPointerException("Arguments can't be null");
            }
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean apply(Map.Entry entry) {
            Comparable entryValue = readAttribute(entry);
            if (entryValue == null) {
                return false;
            }
            Comparable fromConvertedValue = convert(entry, entryValue, from);
            Comparable toConvertedValue = convert(entry, entryValue, to);
            if (fromConvertedValue == null || toConvertedValue == null) {
                return false;
            }
            return entryValue.compareTo(fromConvertedValue) >= 0 && entryValue.compareTo(toConvertedValue) <= 0;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            return index.getSubRecordsBetween(from, to);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeObject(to);
            out.writeObject(from);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            to = in.readObject();
            from = in.readObject();
        }

        @Override
        public String toString() {
            return attribute + " BETWEEN " + from + " AND " + to;
        }
    }

    /**
     * Not Predicate
     */
    public static class NotPredicate implements Predicate, DataSerializable {
        private Predicate predicate;

        public NotPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        public NotPredicate() {
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return !predicate.apply(mapEntry);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(predicate);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            predicate = in.readObject();
        }

        @Override
        public String toString() {
            return "NOT(" + predicate + ")";
        }
    }

    /**
     * In Predicate
     */
    public static class InPredicate extends AbstractPredicate {
        private Comparable[] values;
        private volatile Set<Comparable> convertedInValues;

        public InPredicate() {
        }

        public InPredicate(String attribute, Comparable... values) {
            super(attribute);

            if (values == null) {
                throw new NullPointerException("Array can't be null");
            }
            this.values = values;
        }

        @Override
        public boolean apply(Map.Entry entry) {
            Comparable entryValue = readAttribute(entry);
            if (entryValue == null) {
                return false;
            }
            Set<Comparable> set = convertedInValues;
            if (set == null) {
                set = new HashSet<Comparable>(values.length);
                for (Comparable value : values) {
                    set.add(convert(entry, entryValue, value));
                }
                convertedInValues = set;
            }
            return set.contains(entryValue);
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            if (index != null) {
                return index.getRecords(values);
            } else {
                return null;
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeInt(values.length);
            for (Object value : values) {
                out.writeObject(value);
            }
        }

        @Override
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
            final StringBuilder sb = new StringBuilder();
            sb.append(attribute);
            sb.append(" IN (");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(values[i]);
            }
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Regex Predicate
     */
    public static class RegexPredicate implements Predicate, DataSerializable {
        private String attribute;
        private String regex;
        private volatile Pattern pattern;

        public RegexPredicate() {
        }

        public RegexPredicate(String attribute, String regex) {
            this.attribute = attribute;
            this.regex = regex;
        }

        @Override
        public boolean apply(Map.Entry entry) {
            Comparable attribute = readAttribute(entry, this.attribute);
            String firstVal = attribute == IndexImpl.NULL ? null : (String) attribute;
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

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
            out.writeUTF(regex);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
            regex = in.readUTF();
        }

        @Override
        public String toString() {
            return attribute + " REGEX '" + regex + "'";
        }
    }

    /**
     * Like Predicate
     */
    public static class LikePredicate implements Predicate, DataSerializable {
        protected String attribute;
        protected String second;
        private volatile Pattern pattern;

        public LikePredicate() {
        }

        public LikePredicate(String attribute, String second) {
            this.attribute = attribute;
            this.second = second;
        }

        @Override
        public boolean apply(Map.Entry entry) {
            Comparable attribute = readAttribute(entry, this.attribute);
            String firstVal = attribute == IndexImpl.NULL ? null : (String) attribute;
            if (firstVal == null) {
                return (second == null);
            } else if (second == null) {
                return false;
            } else {
                if (pattern == null) {
                    // we quote the input string then escape then replace % and _
                    // at the end we have a regex pattern look like : \QSOME_STRING\E.*\QSOME_OTHER_STRING\E
                    final String quoted = Pattern.quote(second);
                    String regex = quoted
                            //escaped %
                            .replaceAll("(?<!\\\\)[%]", "\\\\E.*\\\\Q")
                                    //escaped _
                            .replaceAll("(?<!\\\\)[_]", "\\\\E.\\\\Q")
                                    //non escaped %
                            .replaceAll("\\\\%", "%")
                                    //non escaped _
                            .replaceAll("\\\\_", "_");
                    int flags = getFlags();
                    pattern = Pattern.compile(regex, flags);
                }
                Matcher m = pattern.matcher(firstVal);
                return m.matches();
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
            out.writeUTF(second);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
            second = in.readUTF();
        }


        protected int getFlags() {
            //no flags
            return 0;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(attribute)
                    .append(" LIKE '")
                    .append(second)
                    .append("'");
            return builder.toString();
        }
    }

    /**
     * Ilike Predicate
     */
    public static class ILikePredicate extends LikePredicate {

        public ILikePredicate() {
        }

        public ILikePredicate(String attribute, String second) {
            super(attribute, second);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(attribute)
                    .append(" ILIKE '")
                    .append(second)
                    .append("'");
            return builder.toString();
        }


        @Override
        protected int getFlags() {
            return Pattern.CASE_INSENSITIVE;
        }
    }

    /**
     * And Predicate
     */
    public static class AndPredicate implements IndexAwarePredicate, DataSerializable {

        protected Predicate[] predicates;

        public AndPredicate() {
        }

        public AndPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
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

        @Override
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

        @Override
        public boolean apply(Map.Entry mapEntry) {
            for (Predicate predicate : predicates) {
                if (!predicate.apply(mapEntry)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
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
            for (Predicate predicate : predicates) {
                out.writeObject(predicate);
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

    /**
     * Or Predicate
     */
    public static class OrPredicate implements IndexAwarePredicate, DataSerializable {

        private Predicate[] predicates;

        public OrPredicate() {
        }

        public OrPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            List<Set<QueryableEntry>> indexedResults = new LinkedList<Set<QueryableEntry>>();
            for (Predicate predicate : predicates) {
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (iap.isIndexed(queryContext)) {
                        Set<QueryableEntry> s = iap.filter(queryContext);
                        if (s != null) {
                            indexedResults.add(s);
                        }
                    } else {
                        return null;
                    }
                }
            }
            return indexedResults.isEmpty() ? null : new OrResultSet(indexedResults);
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            for (Predicate predicate : predicates) {
                if (predicate instanceof IndexAwarePredicate) {
                    IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
                    if (!iap.isIndexed(queryContext)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            for (Predicate predicate : predicates) {
                if (predicate.apply(mapEntry)) {
                    return true;
                }
            }
            return false;
        }


        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(predicates.length);
            for (Predicate predicate : predicates) {
                out.writeObject(predicate);
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
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
    }

    /**
     * Greater Less Predicate
     */
    public static class GreaterLessPredicate extends EqualPredicate {
        boolean equal;
        boolean less;

        public GreaterLessPredicate() {
        }

        public GreaterLessPredicate(String attribute, Comparable value, boolean equal, boolean less) {
            super(attribute);

            if (value == null) {
                throw new NullPointerException("Arguments can't be null");
            }

            this.value = value;
            this.equal = equal;
            this.less = less;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            final Comparable entryValue = readAttribute(mapEntry);
            if (entryValue == null) {
                return false;
            }
            final Comparable attributeValue = convert(mapEntry, entryValue, value);
            final int result = entryValue.compareTo(attributeValue);
            return equal && result == 0 || (less ? (result < 0) : (result > 0));
        }

        @Override
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
            final StringBuilder sb = new StringBuilder();
            sb.append(attribute);
            sb.append(less ? "<" : ">");
            if (equal) {
                sb.append("=");
            }
            sb.append(value);
            return sb.toString();
        }
    }

    /**
     * Not Equal Predicate
     */
    public static class NotEqualPredicate extends EqualPredicate {
        public NotEqualPredicate() {
        }

        public NotEqualPredicate(String attribute, Comparable value) {
            super(attribute, value);
        }

        @Override
        public boolean apply(Map.Entry entry) {
            return !super.apply(entry);
        }

        @Override
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

    /**
     * Equal Predicate
     */
    public static class EqualPredicate extends AbstractPredicate {
        protected Comparable value;

        public EqualPredicate() {
        }

        public EqualPredicate(String attribute) {
            super(attribute);
        }

        public EqualPredicate(String attribute, Comparable value) {
            super(attribute);
            this.value = value;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = getIndex(queryContext);
            return index.getRecords(value);
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            Comparable entryValue = readAttribute(mapEntry);
            if (entryValue == null) {
                return value == null || value == IndexImpl.NULL;
            }
            value = convert(mapEntry, entryValue, value);
            return entryValue.equals(value);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeObject(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            value = in.readObject();
        }

        @Override
        public String toString() {
            return attribute + "=" + value;
        }
    }

    /**
     * Provides some functionality for some predicates
     * such as Between, In.
     */
    public abstract static class AbstractPredicate implements IndexAwarePredicate, DataSerializable {

        protected String attribute;
        private transient volatile AttributeType attributeType;

        protected AbstractPredicate() {
        }

        protected AbstractPredicate(String attribute) {
            this.attribute = attribute;
        }

        protected Comparable convert(Map.Entry mapEntry, Comparable entryValue, Comparable attributeValue) {
            if (attributeValue == null) {
                return null;
            }
            if (attributeValue instanceof IndexImpl.NullObject) {
                return IndexImpl.NULL;
            }
            AttributeType type = attributeType;
            if (type == null) {
                QueryableEntry queryableEntry = (QueryableEntry) mapEntry;
                type = queryableEntry.getAttributeType(attribute);
                attributeType = type;
            }
            if (type == AttributeType.ENUM) {
                // if attribute type is enum, convert given attribute to enum string
                return type.getConverter().convert(attributeValue);
            } else {
                // if given attribute value is already in expected type then there's no need for conversion.
                if (entryValue != null && entryValue.getClass().isAssignableFrom(attributeValue.getClass())) {
                    return attributeValue;
                } else if (type != null) {
                    return type.getConverter().convert(attributeValue);
                } else {
                    throw new QueryException("Unknown attribute type: " + attributeValue.getClass());
                }
            }
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            return getIndex(queryContext) != null;
        }

        protected Index getIndex(QueryContext queryContext) {
            return queryContext.getIndex(attribute);
        }

        protected Comparable readAttribute(Map.Entry entry) {
            QueryableEntry queryableEntry = (QueryableEntry) entry;
            Comparable val = queryableEntry.getAttribute(attribute);
            if (val != null && val.getClass().isEnum()) {
                val = val.toString();
            }
            return val;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(attribute);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attribute = in.readUTF();
        }
    }

    private static class InstanceOfPredicate implements Predicate, DataSerializable {
        private Class klass;

        public InstanceOfPredicate(Class klass) {
            this.klass = klass;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            Object value = mapEntry.getValue();
            if (value == null) {
                return false;
            }
            return klass.isAssignableFrom(value.getClass());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(klass.getName());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            String klassName = in.readUTF();
            try {
                klass = in.getClassLoader().loadClass(klassName);
            } catch (ClassNotFoundException e) {
                throw new HazelcastSerializationException("Failed to load class: " + klass, e);
            }
        }

        @Override
        public String toString() {
            return " instanceOf (" + klass.getName() + ")";
        }
    }
}
