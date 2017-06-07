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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;

/**
 * Identified version of AbstractPredicateBuilder or non-java clients to be able to
 * support predicate builder.
 */
@BinaryInterface
public class CompositePredicate extends AbstractPredicateBuilder implements IdentifiedDataSerializable {

    public CompositePredicate() {

    }

    public CompositePredicate(PredicateBuilder predicateBuilder) {
        this.lsPredicates = predicateBuilder.lsPredicates;
        this.attribute = predicateBuilder.attribute;
    }

    @Override
    public int getFactoryId() {
        return PredicateDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.COMPOSITE_PREDICATE;
    }

}
