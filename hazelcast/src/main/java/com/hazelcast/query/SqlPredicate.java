/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Predicates;
import com.hazelcast.query.impl.QueryContext;

import java.io.IOException;
import java.util.*;

import static com.hazelcast.query.impl.Predicates.*;

public class SqlPredicate extends AbstractPredicate implements IndexAwarePredicate {
    private transient Predicate predicate;
    private String sql;

    public SqlPredicate(String sql) {
        this.sql = sql;
        predicate = createPredicate(sql);
    }

    public SqlPredicate() {
    }

    public boolean apply(MapEntry mapEntry) {
        return predicate.apply(mapEntry);
    }

    public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index> mapIndexes) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).collectIndexAwarePredicates(lsIndexPredicates, mapIndexes);
        }
        return false;
    }

    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }

    public Set<MapEntry> filter(QueryContext queryContext) {
        return ((IndexAwarePredicate) predicate).filter(queryContext);
    }

    public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index> mapIndexes) {
        if (predicate instanceof IndexAwarePredicate) {
            ((IndexAwarePredicate) predicate).collectAppliedIndexes(setAppliedIndexes, mapIndexes);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(sql);
    }

    public void readData(ObjectDataInput in) throws IOException {
        sql = in.readUTF();
        predicate = createPredicate(sql);
    }

    private int getApostropheIndex(String str, int start) {
        return str.indexOf("'", start);
    }

    private Predicate createPredicate(String sql) {
        Map<String, String> mapPhrases = new HashMap<String, String>(1);
        int apoIndex = getApostropheIndex(sql, 0);
        if (apoIndex != -1) {
            int phraseId = 0;
            StringBuilder newSql = new StringBuilder();
            while (apoIndex != -1) {
                phraseId++;
                int start = apoIndex + 1;
                int end = getApostropheIndex(sql, apoIndex + 1);
                if (end == -1) {
                    throw new RuntimeException("Missing ' in sql");
                }
                String phrase = sql.substring(start, end);
                String key = "$" + phraseId;
                mapPhrases.put(key, phrase);
                String before = sql.substring(0, apoIndex);
                sql = sql.substring(end + 1);
                newSql.append(before);
                newSql.append(key);
                apoIndex = getApostropheIndex(sql, 0);
            }
            newSql.append(sql);
            sql = newSql.toString();
        }
        Parser parser = new Parser();
        List<String> sqlTokens = parser.toPrefix(sql);
        List<Object> tokens = new ArrayList<Object>(sqlTokens);
        if (tokens.size() == 0) throw new RuntimeException("Invalid SQL: [" + sql + "]");
        if (tokens.size() == 1) {
            return eval(tokens.get(0));
        }
        root:
        while (tokens.size() > 1) {
            boolean foundOperand = false;
            for (int i = 0; i < tokens.size(); i++) {
                Object tokenObj = tokens.get(i);
                if (tokenObj instanceof String && parser.isOperand((String) tokenObj)) {
                    foundOperand = true;
                    String token = (String) tokenObj;
                    if ("=".equals(token) || "==".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, equal(get((String) first), second));
                    } else if ("!=".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, notEqual(get((String) first), second));
                    } else if (">".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, greaterThan(get((String) first), (Comparable) second));
                    } else if (">=".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, greaterEqual(get((String) first), (Comparable) second));
                    } else if ("<=".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, lessEqual(get((String) first), (Comparable) second));
                    } else if ("<".equals(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, lessThan(get((String) first), (Comparable) second));
                    } else if ("LIKE".equalsIgnoreCase(token)) {
                        int position = (i - 2);
                        validateOperandPosition(position);
                        Object first = toValue(tokens.remove(position), mapPhrases);
                        Object second = toValue(tokens.remove(position), mapPhrases);
                        setOrAdd(tokens, position, like(get((String) first), (String) second));
                    } else if ("IN".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        validateOperandPosition(position);
                        Object exp = toValue(tokens.remove(position), mapPhrases);
                        String[] values = toValue(((String) tokens.remove(position)).split(","), mapPhrases);
                        setOrAdd(tokens, position, Predicates.in(get((String) exp), values));
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
                        setOrAdd(tokens, position, between(get((String) expression), (Comparable) from, (Comparable) to));
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
                    } else throw new RuntimeException("Unknown token " + token);
                    continue root;
                }
            }
            if (!foundOperand) {
                throw new RuntimeException("Invalid SQL: [" + sql + "]");
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
            if (value != null) keys[i] = value;
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
            return equal(get((String) statement), "true");
        } else {
            return (Predicate) statement;
        }
    }

    @Override
    public String toString() {
        return predicate.toString();
    }
}
