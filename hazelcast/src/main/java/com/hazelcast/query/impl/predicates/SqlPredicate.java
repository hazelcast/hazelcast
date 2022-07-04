/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.util.collection.ArrayUtils;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;
import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.ilike;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.regex;

/**
 * This class contains methods related to conversion of 'sql' query to predicate.
 */
@BinaryInterface
@SuppressWarnings("checkstyle:methodcount")
public class SqlPredicate
        implements IndexAwarePredicate, VisitablePredicate, IdentifiedDataSerializable {

    /**
     * Disables the skip index syntax. The only reason why this property should be set to true is if
     * someone is using % as first character in their fieldname and do not want it to be interpreted
     * as 'skipIndex'.
     */
    private static final boolean SKIP_INDEX_ENABLED = !Boolean.getBoolean("hazelcast.query.disableSkipIndex");

    private static final long serialVersionUID = 1;

    private interface ComparisonPredicateFactory {
        Predicate create(String attribute, Comparable c);
    }

    private static final ComparisonPredicateFactory EQUAL_FACTORY = Predicates::equal;

    private static final ComparisonPredicateFactory NOT_EQUAL_FACTORY = Predicates::notEqual;

    private static final ComparisonPredicateFactory GREATER_THAN_FACTORY = Predicates::greaterThan;

    private static final ComparisonPredicateFactory GREATER_EQUAL_FACTORY = Predicates::greaterEqual;

    private static final ComparisonPredicateFactory LESS_EQUAL_FACTORY = Predicates::lessEqual;

    private static final ComparisonPredicateFactory LESS_THAN_FACTORY = Predicates::lessThan;

    transient Predicate predicate;
    private String sql;

    public SqlPredicate(String sql) {
        this.sql = sql;
        predicate = createPredicate(sql);
    }

    public SqlPredicate() {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
        }
        return false;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).filter(queryContext);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(sql);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sql = in.readString();
        predicate = createPredicate(sql);
    }

    private int getApostropheIndex(String str, int start) {
        return str.indexOf('\'', start);
    }

    private int getApostropheIndexIgnoringDoubles(String str, int start) {
        int i = str.indexOf('\'', start);
        int j = str.indexOf('\'', i + 1);
        //ignore doubles
        while (i == j - 1) {
            i = str.indexOf('\'', j + 1);
            j = str.indexOf('\'', i + 1);
        }
        return i;
    }

    private String removeEscapes(String phrase) {
        return (phrase.length() > 2) ? phrase.replace("''", "'") : phrase;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    private Predicate createPredicate(String sql) {
        String paramSql = sql;
        Map<String, String> mapPhrases = new HashMap<>();
        int apoIndex = getApostropheIndex(paramSql, 0);
        if (apoIndex != -1) {
            int phraseId = 0;
            StringBuilder newSql = new StringBuilder();
            while (apoIndex != -1) {
                phraseId++;
                int start = apoIndex + 1;
                int end = getApostropheIndexIgnoringDoubles(paramSql, apoIndex + 1);
                if (end == -1) {
                    throw new IllegalArgumentException("Missing ' in sql");
                }
                String phrase = removeEscapes(paramSql.substring(start, end));

                String key = "$" + phraseId;
                mapPhrases.put(key, phrase);
                String before = paramSql.substring(0, apoIndex);
                paramSql = paramSql.substring(end + 1);
                newSql.append(before);
                newSql.append(key);
                apoIndex = getApostropheIndex(paramSql, 0);
            }
            newSql.append(paramSql);
            paramSql = newSql.toString();
        }
        SqlParser parser = new SqlParser();
        List<String> sqlTokens = parser.toPrefix(paramSql);
        List<Object> tokens = new ArrayList<>(sqlTokens);
        if (tokens.size() == 0) {
            throw new IllegalArgumentException("Invalid SQL: [" + paramSql + "]");
        }
        if (tokens.size() == 1) {
            return eval(tokens.get(0));
        }
        root:
        while (tokens.size() > 1) {
            boolean foundOperand = false;
            for (int i = 0; i < tokens.size(); i++) {
                Object tokenObj = tokens.get(i);
                if (tokenObj instanceof String && parser.isOperand((String) tokenObj)) {
                    String token = (String) tokenObj;
                    if ("=".equals(token) || "==".equals(token)) {
                        createComparison(mapPhrases, tokens, i, EQUAL_FACTORY);
                    } else if ("!=".equals(token) || "<>".equals(token)) {
                        createComparison(mapPhrases, tokens, i, NOT_EQUAL_FACTORY);
                    } else if (">".equals(token)) {
                        createComparison(mapPhrases, tokens, i, GREATER_THAN_FACTORY);
                    } else if (">=".equals(token)) {
                        createComparison(mapPhrases, tokens, i, GREATER_EQUAL_FACTORY);
                    } else if ("<=".equals(token)) {
                        createComparison(mapPhrases, tokens, i, LESS_EQUAL_FACTORY);
                    } else if ("<".equals(token)) {
                        createComparison(mapPhrases, tokens, i, LESS_THAN_FACTORY);
                    } else if (equalsIgnoreCase("LIKE", token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, like((String) first, (String) second));
                    } else if (equalsIgnoreCase("ILIKE", token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, ilike((String) first, (String) second));
                    } else if (equalsIgnoreCase("REGEX", token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, regex((String) first, (String) second));
                    } else if (equalsIgnoreCase("IN", token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        String exp = (String) toValue(tokens.remove(position), mapPhrases);
                        String[] values = toValue(((String) tokens.remove(position)).split(","), mapPhrases);

                        if (skipIndex(exp)) {
                            exp = exp.substring(1);
                            setOrAdd(tokens, position, new SkipIndexPredicate(Predicates.in(exp, values)));
                        } else {
                            setOrAdd(tokens, position, Predicates.in(exp, values));
                        }
                    } else if (equalsIgnoreCase("NOT", token)) {
                        int position = i - 1;
                        validateOperandPosition(position);
                        Object exp = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, Predicates.not(eval(exp)));
                    } else if (equalsIgnoreCase("BETWEEN", token)) {
                        int position = i - 3;
                        validateOperandPosition(position);
                        Object expression = tokens.remove(position);
                        Object from = toValue(tokens.remove(position), mapPhrases);
                        Object to = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, between((String) expression, (Comparable) from, (Comparable) to));
                    } else if (equalsIgnoreCase("AND", token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, flattenCompound(eval(first), eval(second), AndPredicate.class));
                    } else if (equalsIgnoreCase("OR", token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, flattenCompound(eval(first), eval(second), OrPredicate.class));
                    } else {
                        throw new IllegalArgumentException("Unknown token " + token);
                    }
                    continue root;
                }
            }
            if (!foundOperand) {
                throw new IllegalArgumentException("Invalid SQL: [" + paramSql + "]");
            }
        }
        return (Predicate) tokens.get(0);
    }

    private void createComparison(Map<String, String> mapPhrases,
                                  List<Object> tokens,
                                  int i,
                                  ComparisonPredicateFactory factory) {
        int position = (i - 2);
        validateOperandPosition(position);
        String first = (String) toValue(tokens.remove(position), mapPhrases);
        Comparable second = (Comparable) toValue(tokens.remove(position), mapPhrases);

        if (skipIndex(first)) {
            first = first.substring(1);
            setOrAdd(tokens, position, new SkipIndexPredicate(factory.create(first, second)));
        } else {
            setOrAdd(tokens, position, factory.create(first, second));
        }
    }

    private boolean skipIndex(String first) {
        return SKIP_INDEX_ENABLED && first.startsWith("%");
    }

    private void validateOperandPosition(int pos) {
        if (pos < 0) {
            throw new IllegalArgumentException("Invalid SQL: [" + sql + "]");
        }
    }

    private Object toValue(final Object key, final Map<String, String> phrases) {
        final String value = phrases.get(key);
        if (value != null) {
            return value;
        } else if (key instanceof String && (equalsIgnoreCase("null", (String) key))) {
            return null;
        } else {
            return key;
        }
    }

    private String[] toValue(final String[] keys, final Map<String, String> phrases) {
        for (int i = 0; i < keys.length; i++) {
            final String value = phrases.get(keys[i]);
            if (value != null) {
                keys[i] = value;
            }
        }
        return keys;
    }

    private void setOrAdd(List tokens, int position, Predicate predicate) {
        if (tokens.size() == 0) {
            tokens.add(predicate);
        } else {
            tokens.set(position, predicate);
        }
    }

    private Predicate eval(Object statement) {
        if (statement instanceof String) {
            return equal((String) statement, "true");
        } else {
            return (Predicate) statement;
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        predicate = createPredicate(sql);
    }

    /**
     * Return a {@link CompoundPredicate}, possibly flattened if one or both arguments is an instance of
     * {@code CompoundPredicate}.
     */
    static <T extends CompoundPredicate> T flattenCompound(Predicate predicateLeft, Predicate predicateRight, Class<T> klass) {
        // The following could have been achieved with {@link com.hazelcast.query.impl.predicates.FlatteningVisitor},
        // however since we only care for 2-argument flattening, we can avoid constructing a visitor and its internals
        // for each token pass at the cost of the following explicit code.
        Predicate[] predicates;
        if (klass.isInstance(predicateLeft) || klass.isInstance(predicateRight)) {
            Predicate[] left = getSubPredicatesIfClass(predicateLeft, klass);
            Predicate[] right = getSubPredicatesIfClass(predicateRight, klass);

            predicates = new Predicate[left.length + right.length];
            ArrayUtils.concat(left, right, predicates);
        } else {
            predicates = new Predicate[]{predicateLeft, predicateRight};
        }
        try {
            T compoundPredicate = klass.newInstance();
            compoundPredicate.setPredicates(predicates);
            return compoundPredicate;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException(String.format("%s must have a public default constructor", klass.getName()));
        }
    }

    private static <T extends CompoundPredicate> Predicate[] getSubPredicatesIfClass(Predicate predicate, Class<T> klass) {
        if (klass.isInstance(predicate)) {
            return ((CompoundPredicate) predicate).getPredicates();
        } else {
            return new Predicate[]{predicate};
        }
    }

    @Override
    public String toString() {
        return predicate.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SqlPredicate)) {
            return false;
        }

        SqlPredicate that = (SqlPredicate) o;

        return sql.equals(that.sql);
    }

    @Override
    public int hashCode() {
        return sql.hashCode();
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        Predicate target = predicate;
        if (predicate instanceof VisitablePredicate) {
            target = ((VisitablePredicate) predicate).accept(visitor, indexes);
        }
        return target;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.SQL_PREDICATE;
    }
}
