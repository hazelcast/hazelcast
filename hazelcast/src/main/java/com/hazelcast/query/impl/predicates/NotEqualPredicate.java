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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;
import java.util.Set;

/**
 * Not Equal Predicate
 */
@BinaryInterface
public final class NotEqualPredicate extends EqualPredicate {

    private static final long serialVersionUID = 1L;

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
    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return null;
    }

    @Override
    public String toString() {
        return attributeName + " != " + value;
    }

    @Override
    public Predicate negate() {
        return new EqualPredicate(attributeName, value);
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.NOTEQUAL_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof NotEqualPredicate)) {
            return false;
        }

        NotEqualPredicate that = (NotEqualPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof NotEqualPredicate);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
