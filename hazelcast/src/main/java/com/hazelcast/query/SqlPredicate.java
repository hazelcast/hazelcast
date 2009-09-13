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
        System.out.println(sql);
        root:
        while (tokens.size() > 1) {
            for (int i = 0; i < tokens.size(); i++) {
                Object tokenObj = tokens.get(i);
                if (tokenObj instanceof String && Parser.isPrecedence((String) tokenObj)) {
                    String token = (String) tokenObj;
                    if ("=".equals(token) || "==".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = equal(get((String) first), second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("!=".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = notEqual(get((String) first), second);
                        removeThreeAddOne(tokens, i, p);
                    } else if (">".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = greaterThan(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if (">=".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = greaterEqual(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("<=".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = lessEqual(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("<".equals(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = lessThan(get((String) first), (Comparable) second);
                        removeThreeAddOne(tokens, i, p);
                    } else if ("BETWEEN".equalsIgnoreCase(token)) {
                        Object expression = tokens.remove(i - 2);
                        Object from = tokens.remove(i - 2);
                        Object between = tokens.remove(i - 2);
                        Object and = tokens.remove(i - 2);
                        Object to = tokens.remove(i - 2);
                        tokens.add((i - 2), between(get((String) expression), (Comparable) from, (Comparable) to));
                    } else if ("AND".equalsIgnoreCase(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = Predicates.and(eval(first), eval(second));
                        removeThreeAddOne(tokens, i, p);
                    } else if ("OR".equalsIgnoreCase(token)) {
                        Object first = tokens.get(i - 2);
                        Object second = tokens.get(i - 1);
                        Predicate p = Predicates.or(eval(first), eval(second));
                        removeThreeAddOne(tokens, i, p);
                    } else throw new RuntimeException("Unknown token " + token);
                    continue root;
                }
            }
        }
        System.out.println(tokens.get(0));
        return (Predicate) tokens.get(0);
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
}
