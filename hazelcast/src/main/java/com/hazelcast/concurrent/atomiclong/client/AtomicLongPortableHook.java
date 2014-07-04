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

package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.nio.serialization.AbstractPortableHook;
import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.util.ConstructorFunction;

public class AtomicLongPortableHook extends AbstractPortableHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.ATOMIC_LONG_PORTABLE_FACTORY, -17);

    static final int ADD_AND_GET = 1;
    static final int COMPARE_AND_SET = 2;
    static final int GET_AND_ADD = 3;
    static final int GET_AND_SET = 4;
    static final int SET = 5;
    static final int APPLY = 6;
    static final int ALTER = 7;
    static final int ALTER_AND_GET = 8;
    static final int GET_AND_ALTER = 9;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[GET_AND_ALTER + 1];
        constructors[ADD_AND_GET] = createFunction(new AddAndGetRequest());
        constructors[COMPARE_AND_SET] = createFunction(new CompareAndSetRequest());
        constructors[GET_AND_ADD] = createFunction(new GetAndAddRequest());
        constructors[GET_AND_SET] = createFunction(new GetAndSetRequest());
        constructors[SET] = createFunction(new SetRequest());
        constructors[APPLY] = createFunction(new ApplyRequest());
        constructors[ALTER] = createFunction(new AlterRequest());
        constructors[ALTER_AND_GET] = createFunction(new AlterAndGetRequest());
        constructors[GET_AND_ALTER] = createFunction(new GetAndAlterRequest());
        return new ArrayPortableFactory(constructors);
    }
}
