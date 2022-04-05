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
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Ilike Predicate
 */
@BinaryInterface
public class ILikePredicate extends LikePredicate {

    public ILikePredicate() {
    }

    public ILikePredicate(String attribute, String second) {
        super(attribute, second);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return null;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }

    @Override
    public String toString() {
        return attributeName + " ILIKE '" + expression + "'";
    }

    @Override
    protected int getFlags() {
        return super.getFlags() | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.ILIKE_PREDICATE;
    }
}
