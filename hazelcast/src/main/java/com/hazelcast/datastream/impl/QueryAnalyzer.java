/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class QueryAnalyzer {

    private final RecordModel recordModel;
    private Set<String> variables = new HashSet<String>();
    private Set<String> attributes = new HashSet<String>();
    private List<EqualPredicate> equalPredicates = new LinkedList<EqualPredicate>();

    public QueryAnalyzer(Predicate predicate, RecordModel recordModel) {
        this.recordModel = recordModel;
        collect(predicate);

    }

    public Set<String> getParameters() {
        return variables;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public EqualPredicate getIndexedEqualPredicate() {
        for (EqualPredicate equalPredicate : equalPredicates) {
            String attribute = equalPredicate.getAttributeName();
            if (recordModel.getIndexFields().contains(attribute)) {
                return equalPredicate;
            }
        }
        return null;
    }

    public void collect(Predicate predicate) {
        if (predicate instanceof SqlPredicate) {
            collect((SqlPredicate) predicate);
        } else if (predicate instanceof TruePredicate) {
        } else if (predicate instanceof NotPredicate) {
            collect((NotPredicate) predicate);
        } else if (predicate instanceof AndPredicate) {
            collect((AndPredicate) predicate);
        } else if (predicate instanceof OrPredicate) {
            collect((OrPredicate) predicate);
        } else if (predicate instanceof BetweenPredicate) {
            collect((BetweenPredicate) predicate);
        } else if (predicate instanceof NotEqualPredicate) {
            collect((NotEqualPredicate) predicate);
        } else if (predicate instanceof EqualPredicate) {
            collect((EqualPredicate) predicate);
        } else if (predicate instanceof GreaterLessPredicate) {
            collect((GreaterLessPredicate) predicate);
        } else {
            throw new RuntimeException("Unhandled predicate:" + predicate.getClass());
        }
    }

    private void collect(GreaterLessPredicate predicate) {
        attributes.add(predicate.getAttributeName());
        collectValue(predicate.getValue());
    }

    private void collect(EqualPredicate predicate) {
        this.equalPredicates.add(predicate);

        attributes.add(predicate.getAttributeName());
        collectValue(predicate.getValue());
    }

    private void collect(NotEqualPredicate predicate) {
        attributes.add(predicate.getAttributeName());
        collectValue(predicate.getValue());

    }

    private void collect(BetweenPredicate predicate) {
        attributes.add(predicate.getAttributeName());
        collectValue(predicate.getFrom());
        collectValue(predicate.getTo());
    }

    private void collect(OrPredicate predicate) {
        for (Predicate p : predicate.getPredicates()) {
            collect(p);
        }
    }

    private void collect(AndPredicate predicate) {
        for (Predicate p : predicate.getPredicates()) {
            collect(p);
        }
    }

    private void collect(NotPredicate predicate) {
        collect(predicate.negate());
    }

    private void collect(SqlPredicate predicate) {
        collect(predicate.getPredicate());
    }

    private void collectValue(Object value) {
        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.startsWith("$")) {
                String variableName = valueString.substring(1);
                variables.add(variableName);
            }
        }
    }
}
