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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.internal.util.ConstructorFunction;

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

    public static final int LEN = POSITIVE_INFINITY + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[SQL_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SqlPredicate();
            }
        };
        constructors[AND_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AndPredicate();
            }
        };
        constructors[BETWEEN_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BetweenPredicate();
            }
        };
        constructors[EQUAL_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EqualPredicate();
            }
        };
        constructors[GREATERLESS_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new GreaterLessPredicate();
            }
        };
        constructors[LIKE_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LikePredicate();
            }
        };
        constructors[ILIKE_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ILikePredicate();
            }
        };
        constructors[IN_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new InPredicate();
            }
        };
        constructors[INSTANCEOF_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new InstanceOfPredicate();
            }
        };
        constructors[NOTEQUAL_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NotEqualPredicate();
            }
        };
        constructors[NOT_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NotPredicate();
            }
        };
        constructors[OR_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new OrPredicate();
            }
        };
        constructors[REGEX_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RegexPredicate();
            }
        };
        constructors[FALSE_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return FalsePredicate.INSTANCE;
            }
        };
        constructors[TRUE_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return TruePredicate.INSTANCE;
            }
        };
        constructors[PAGING_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PagingPredicateImpl();
            }
        };
        constructors[PARTITION_PREDICATE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionPredicateImpl();
            }
        };
        constructors[NULL_OBJECT] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return IndexImpl.NULL;
            }
        };
        constructors[COMPOSITE_VALUE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CompositeValue();
            }
        };
        constructors[NEGATIVE_INFINITY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return CompositeValue.NEGATIVE_INFINITY;
            }
        };
        constructors[POSITIVE_INFINITY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return CompositeValue.POSITIVE_INFINITY;
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
