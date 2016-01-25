/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

/**
 * reminder:
 * when FalsePredicate is going to implement IdentifiedDataSerializable, make sure no new instance
 * is created, but the INSTANCE is returned. No need to create new objects.
 */
public class FalsePredicate implements IdentifiedDataSerializable, Predicate, IndexAwarePredicate {
    /**
     * An instance of the FalsePredicate.
     */
    public static final FalsePredicate INSTANCE = new FalsePredicate();

     @Override
    public boolean apply(Map.Entry mapEntry) {
        return false;
    }

    @Override
    public String toString() {
        return "FalsePredicate{}";
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return Collections.emptySet();
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.FALSE_PREDICATE;
    }
}
