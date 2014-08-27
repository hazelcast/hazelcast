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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.ConnectorPredicate;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.ilike;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.Predicates.regex;

/**
 * This class contains methods related to conversion of sql query to predicate.
 */

public class SqlPredicate extends AbstractPredicate implements IndexAwarePredicate, ConnectorPredicate {

    private static final long serialVersionUID = 1;

    private transient ConnectorPredicate predicate;
    private String sql;

    public SqlPredicate(String sql) {
        this.sql = sql;
        // it's likely a ConnectorPredicate but in order to be sure
        // we're wrapping generated Predicate with AndPredicate which has no effect on result.
        predicate = createPredicate(sql);
    }

    public SqlPredicate() {
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    @Override
    public boolean in(Predicate predicate) {
        return this.predicate.in(predicate);
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
        predicate = new AndPredicate(createPredicate(sql));
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

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).isIndexed(queryContext);
    }

    private String removeEscapes(String phrase) {
        return (phrase.length() > 2) ? phrase.replace("''", "'") : phrase;
    }

    private ConnectorPredicate createPredicate(String sql) {
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
            return new AndPredicate(eval(tokens.get(0)));
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
                    } else if ("!=".equals(token)) {
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
                        setOrAdd(tokens, position, and(eval(first), eval(second)));
                    } else if ("OR".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, or(eval(first), eval(second)));
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

        Object o = tokens.get(0);
        if (!(o instanceof ConnectorPredicate)) {
            return new AndPredicate((Predicate) o);
        }
        return (ConnectorPredicate) o;
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
            return IndexImpl.NULL;
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
        predicate = new AndPredicate(createPredicate(sql));
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
    public ConnectorPredicate subtract(Predicate predicates) {
        return predicate.subtract(predicates);
    }

    @Override
    public ConnectorPredicate copy() {
        return predicate.copy();
    }

    @Override
    public void removeChild(int index) {
        predicate.removeChild(index);
    }

    @Override
    public int getPredicateCount() {
        return predicate.getPredicateCount();
    }

    @Override
    public Predicate[] getPredicates() {
        return predicate.getPredicates();
    }

    @Override
    public boolean isSubset(Predicate predicate) {
        return this.predicate.isSubset(predicate);
    }
}
