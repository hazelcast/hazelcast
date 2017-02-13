/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.CompoundPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import com.hazelcast.query.impl.predicates.Visitor;
import com.hazelcast.util.collection.ArrayUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.ilike;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.regex;

/**
 * This class contains methods related to conversion of sql query to predicate.
 */
@BinaryInterface
public class SqlPredicate
        implements IndexAwarePredicate, VisitablePredicate, IdentifiedDataSerializable {

    private static final long serialVersionUID = 1;

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
        out.writeUTF(sql);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sql = in.readUTF();
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

    private Predicate createPredicate(String sql) {
        String paramSql = sql;
        Map<String, String> mapPhrases = new HashMap<String, String>(1);
        int apoIndex = getApostropheIndex(paramSql, 0);
        if (apoIndex != -1) {
            int phraseId = 0;
            StringBuilder newSql = new StringBuilder();
            while (apoIndex != -1) {
                phraseId++;
                int start = apoIndex + 1;
                int end = getApostropheIndexIgnoringDoubles(paramSql, apoIndex + 1);
                if (end == -1) {
                    throw new RuntimeException("Missing ' in sql");
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
        Parser parser = new Parser();
        List<String> sqlTokens = parser.toPrefix(paramSql);
        List<Object> tokens = new ArrayList<Object>(sqlTokens);
        if (tokens.size() == 0) {
            throw new RuntimeException("Invalid SQL: [" + paramSql + "]");
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
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, equal((String) first, (Comparable) second));
                    } else if ("!=".equals(token) || "<>".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, notEqual((String) first, (Comparable) second));
                    } else if (">".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, greaterThan((String) first, (Comparable) second));
                    } else if (">=".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, greaterEqual((String) first, (Comparable) second));
                    } else if ("<=".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, lessEqual((String) first, (Comparable) second));
                    } else if ("<".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, lessThan((String) first, (Comparable) second));
                    } else if ("LIKE".equalsIgnoreCase(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, like((String) first, (String) second));
                    } else if ("ILIKE".equalsIgnoreCase(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, ilike((String) first, (String) second));
                    } else if ("REGEX".equalsIgnoreCase(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, regex((String) first, (String) second));
                    } else if ("IN".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object exp = toValue(tokens.remove(position), mapPhrases);
                        String[] values = toValue(((String) tokens.remove(position)).split(","), mapPhrases);
                        setOrAdd(tokens, position, Predicates.in((String) exp, values));
                    } else if ("NOT".equalsIgnoreCase(token)) {
                        int position = i - 1;
                        validateOperandPosition(position);
                        Object exp = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, Predicates.not(eval(exp)));
                    } else if ("BETWEEN".equalsIgnoreCase(token)) {
                        int position = i - 3;
                        validateOperandPosition(position);
                        Object expression = tokens.remove(position);
                        Object from = toValue(tokens.remove(position), mapPhrases);
                        Object to = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, between((String) expression, (Comparable) from, (Comparable) to));
                    } else if ("AND".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, flattenCompound(eval(first), eval(second), AndPredicate.class));
                    } else if ("OR".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, flattenCompound(eval(first), eval(second), OrPredicate.class));
                    } else {
                        throw new RuntimeException("Unknown token " + token);
                    }
                    continue root;
                }
            }
            if (!foundOperand) {
                throw new RuntimeException("Invalid SQL: [" + paramSql + "]");
            }
        }
        return (Predicate) tokens.get(0);
    }

    private void validateOperandPosition(int pos) {
        if (pos < 0) {
            throw new RuntimeException("Invalid SQL: [" + sql + "]");
        }
    }

    private Object toValue(final Object key, final Map<String, String> phrases) {
        final String value = phrases.get(key);
        if (value != null) {
            return value;
        } else if (key instanceof String && ("null".equalsIgnoreCase((String) key))) {
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
     *
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
            predicates = new Predicate[] {predicateLeft, predicateRight};
        }
        try {
            CompoundPredicate compoundPredicate = klass.newInstance();
            compoundPredicate.setPredicates(predicates);
            return (T) compoundPredicate;
        } catch (InstantiationException e) {
            throw new RuntimeException(String.format("%s should have a public default constructor", klass.getName()));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("%s should have a public default constructor", klass.getName()));
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
    public int getId() {
        return PredicateDataSerializerHook.SQL_PREDICATE;
    }
}
