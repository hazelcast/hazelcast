/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.query.impl.CompositeValue;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

public class PredicateDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(PREDICATE_DS_FACTORY, PREDICATE_DS_FACTORY_ID);

    public static final int SQL_PREDICATE = 0;
    public static final int AND_PREDICATE = 1;
    public static final int BETWEEN_PREDICATE = 2;
    public static final int EQUAL_PREDICATE = 3;
    public static final int GREATERLESS_PREDICATE = 4;
    public static final int LIKE_PREDICATE = 5;
    public static final int ILIKE_PREDICATE = 6;
    public static final int IN_PREDICATE = 7;
    public static final int INSTANCEOF_PREDICATE = 8;
    public static final int NOTEQUAL_PREDICATE = 9;
    public static final int NOT_PREDICATE = 10;
    public static final int OR_PREDICATE = 11;
    public static final int REGEX_PREDICATE = 12;
    public static final int FALSE_PREDICATE = 13;
    public static final int TRUE_PREDICATE = 14;
    public static final int PAGING_PREDICATE = 15;
    public static final int PARTITION_PREDICATE = 16;
    public static final int NULL_OBJECT = 17;
    // Objects corresponding to the 3 entries bellow are never transferred over
    // the wire.
    public static final int COMPOSITE_VALUE = 18;
    public static final int NEGATIVE_INFINITY = 19;
    public static final int POSITIVE_INFINITY = 20;
    public static final int MULTI_PARTITION_PREDICATE = 21;

    public static final int LEN = MULTI_PARTITION_PREDICATE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[SQL_PREDICATE] = SqlPredicate::new;
        constructors[AND_PREDICATE] = AndPredicate::new;
        constructors[BETWEEN_PREDICATE] = BetweenPredicate::new;
        constructors[EQUAL_PREDICATE] = EqualPredicate::new;
        constructors[GREATERLESS_PREDICATE] = GreaterLessPredicate::new;
        constructors[LIKE_PREDICATE] = LikePredicate::new;
        constructors[ILIKE_PREDICATE] = ILikePredicate::new;
        constructors[IN_PREDICATE] = InPredicate::new;
        constructors[INSTANCEOF_PREDICATE] = InstanceOfPredicate::new;
        constructors[NOTEQUAL_PREDICATE] = NotEqualPredicate::new;
        constructors[NOT_PREDICATE] = NotPredicate::new;
        constructors[OR_PREDICATE] = OrPredicate::new;
        constructors[REGEX_PREDICATE] = RegexPredicate::new;
        constructors[FALSE_PREDICATE] = () -> FalsePredicate.INSTANCE;
        constructors[TRUE_PREDICATE] = () -> TruePredicate.INSTANCE;
        constructors[PAGING_PREDICATE] = PagingPredicateImpl::new;
        constructors[PARTITION_PREDICATE] = PartitionPredicateImpl::new;
        constructors[NULL_OBJECT] = () -> AbstractIndex.NULL;
        constructors[COMPOSITE_VALUE] = CompositeValue::new;
        constructors[NEGATIVE_INFINITY] = () -> CompositeValue.NEGATIVE_INFINITY;
        constructors[POSITIVE_INFINITY] = () -> CompositeValue.POSITIVE_INFINITY;
        constructors[MULTI_PARTITION_PREDICATE] = MultiPartitionPredicateImpl::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
