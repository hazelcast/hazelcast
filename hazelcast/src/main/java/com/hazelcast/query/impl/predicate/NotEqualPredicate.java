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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.ComparisonType;
import com.hazelcast.query.impl.resultset.FilteredResultSet;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.ValidationUtil;

import java.util.Map;
import java.util.Set;

/**
 * Not Equal Predicate
 */
public class NotEqualPredicate extends EqualPredicate {
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
    public boolean isSubSet(Predicate predicate) {
        return equals(predicate);
    }

    @Override
    public boolean equals(Object predicate) {
        if (predicate instanceof NotEqualPredicate) {
            NotEqualPredicate p = (NotEqualPredicate) predicate;
            return ValidationUtil.equalOrNull(p.attribute, attribute) && ValidationUtil.equalOrNull(p.value, value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        if (index != null) {
            Set<QueryableEntry> subRecords = index.getSubRecords(ComparisonType.NOT_EQUAL, value);
            Predicate notIndexedPredicate = queryContext.getQueryPlan(true).getMappedNotIndexedPredicate(this);
            if (notIndexedPredicate != null) {
                return new FilteredResultSet(subRecords, notIndexedPredicate);
            } else {
                return subRecords;
            }
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return attribute + " != " + value;
    }
}
