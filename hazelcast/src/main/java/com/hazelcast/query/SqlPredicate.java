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
import static com.hazelcast.query.Predicates.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public boolean collectIndexAwarePredicates(List<IndexAwarePredicate> lsIndexPredicates, Map<Expression, Index<MapEntry>> mapIndexes) {
        if (predicate instanceof IndexAwarePredicate) {
            return ((IndexAwarePredicate) predicate).collectIndexAwarePredicates(lsIndexPredicates, mapIndexes);
        }
        return false;
    }

    public Set<MapEntry> filter(Map<Expression, Index<MapEntry>> mapIndexes) {
        return ((IndexAwarePredicate) predicate).filter(mapIndexes);
    }

    public void collectAppliedIndexes(Set<Index> setAppliedIndexes, Map<Expression, Index<MapEntry>> mapIndexes) {
        if (predicate instanceof IndexAwarePredicate) {
            ((IndexAwarePredicate) predicate).collectAppliedIndexes(setAppliedIndexes, mapIndexes);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(sql);
    }

    public void readData(DataInput in) throws IOException {
        sql = in.readUTF();
        predicate = createPredicate(sql);
    }

    private Predicate createPredicate(String sql) {
        Parser parser = new Parser();
        List<Object> tokens = new ArrayList<Object>(parser.toPrefix(sql));
//        System.out.println(sql);
//        System.out.println("token " + tokens);
        if (tokens.size() == 0) throw new RuntimeException("Invalid SQL :" + sql);
        if (tokens.size() == 1) {
            return eval(tokens.get(0));
        }
        root:
        while (tokens.size() > 1) {
//            System.out.println(tokens);
            for (int i = 0; i < tokens.size(); i++) {
                Object tokenObj = tokens.get(i);
//                System.out.println(tokenObj + " ... " + tokenObj.getClass());
                if (tokenObj instanceof String && parser.isOperand((String) tokenObj)) {
                    String token = (String) tokenObj;
                    if ("=".equals(token) || "==".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, equal(get((String) first), second));
                    } else if ("!=".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, notEqual(get((String) first), second));
                    } else if (">".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, greaterThan(get((String) first), (Comparable) second));
                    } else if (">=".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, greaterEqual(get((String) first), (Comparable) second));
                    } else if ("<=".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, lessEqual(get((String) first), (Comparable) second));
                    } else if ("<".equals(token)) {
                        int position = (i - 2);
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, lessThan(get((String) first), (Comparable) second));
                    } else if ("IN".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        Object exp = tokens.remove(position);
                        String[] values = ((String) tokens.remove(position)).split(",");
                        setOrAdd(tokens, position, Predicates.in(get((String) exp), values));
                    } else if ("NOT".equalsIgnoreCase(token)) {
                        int position = i - 1;
                        Object exp = tokens.remove(position);
                        setOrAdd(tokens, position, Predicates.not(eval(exp)));
                    } else if ("BETWEEN".equalsIgnoreCase(token)) {
                        int position = i - 3;
                        Object expression = tokens.remove(position);
                        Object from = tokens.remove(position);
                        Object to = tokens.remove(position);
                        setOrAdd(tokens, position, between(get((String) expression), (Comparable) from, (Comparable) to));
                    } else if ("AND".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, and(eval(first), eval(second)));
                    } else if ("OR".equalsIgnoreCase(token)) {
                        int position = i - 2;
                        Object first = tokens.remove(position);
                        Object second = tokens.remove(position);
                        setOrAdd(tokens, position, or(eval(first), eval(second)));
                    } else throw new RuntimeException("Unknown token " + token);
                    continue root;
                }
            }
        }
        return (Predicate) tokens.get(0);
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

    private void removeThreeAddOne(List<Object> tokens, int i, Object added) {
        tokens.remove(i - 2);
        tokens.remove(i - 2);
        tokens.remove(i - 2);
        tokens.add(i - 2, added);
    }

    @Override
    public String toString() {
        return predicate.toString();
    }
}
