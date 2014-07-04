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

package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.nio.serialization.AbstractPortableHook;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;

public class AtomicReferencePortableHook extends AbstractPortableHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_REFERENCE_PORTABLE_FACTORY, -21);

    static final int GET = 1;
    static final int SET = 2;
    static final int GET_AND_SET = 3;
    static final int IS_NULL = 4;
    static final int COMPARE_AND_SET = 5;
    static final int CONTAINS = 6;
    static final int APPLY = 7;
    static final int ALTER = 8;
    static final int ALTER_AND_GET = 9;
    static final int GET_AND_ALTER = 10;

    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[GET_AND_ALTER + 1];
        constructors[GET] = createFunction(new GetRequest());
        constructors[SET] = createFunction(new SetRequest());
        constructors[GET_AND_SET] = createFunction(new GetAndSetRequest());
        constructors[IS_NULL] = createFunction(new IsNullRequest());
        constructors[COMPARE_AND_SET] = createFunction(new CompareAndSetRequest());
        constructors[CONTAINS] = createFunction(new ContainsRequest());
        constructors[APPLY] = createFunction(new ApplyRequest());
        constructors[ALTER] = createFunction(new AlterRequest());
        constructors[ALTER_AND_GET] = createFunction(new AlterAndGetRequest());
        constructors[GET_AND_ALTER] = createFunction(new GetAndAlterRequest());
        return new ArrayPortableFactory(constructors);
    }
}
