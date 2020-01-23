/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl;

import com.hazelcast.internal.longregister.operations.GetOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.log.impl.operations.ClearOperation;
import com.hazelcast.log.impl.operations.ClearOperationFactory;
import com.hazelcast.log.impl.operations.CountOperation;
import com.hazelcast.log.impl.operations.CountOperationFactory;
import com.hazelcast.log.impl.operations.PutOperation;
import com.hazelcast.log.impl.operations.ReduceOperation;
import com.hazelcast.log.impl.operations.SupplyOperation;
import com.hazelcast.log.impl.operations.SupplyOperationFactory;
import com.hazelcast.log.impl.operations.UsageOperation;
import com.hazelcast.log.impl.operations.UsageOperationFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.LOG_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.LOG_DS_FACTORY_ID;

public class LogDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(LOG_DS_FACTORY, LOG_DS_FACTORY_ID);

    public static final int PUT = 0;
    public static final int GET = 1;
    public static final int SUPPLY = 2;
    public static final int SUPPLY_FACTORY = 3;
    public static final int USAGE = 4;
    public static final int USAGE_FACTORY = 5;
    public static final int REDUCE = 6;
    public static final int COUNT = 7;
    public static final int COUNT_FACTORY = 8;
    public static final int CLEAR = 9;
    public static final int CLEAR_FACTORY = 10;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case PUT:
                    return new PutOperation();
                case GET:
                    return new GetOperation();
                case SUPPLY:
                    return new SupplyOperation();
                case SUPPLY_FACTORY:
                    return new SupplyOperationFactory();
                case USAGE:
                    return new UsageOperation();
                case USAGE_FACTORY:
                    return new UsageOperationFactory();
                case REDUCE:
                    return new ReduceOperation();
                case COUNT:
                    return new CountOperation();
                case COUNT_FACTORY:
                    return new CountOperationFactory();
                case CLEAR:
                    return new ClearOperation();
                case CLEAR_FACTORY:
                    return new ClearOperationFactory();
                default:
                    throw new IllegalArgumentException();
            }
        };
    }
}