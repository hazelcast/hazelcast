/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl;

import com.hazelcast.dictionary.impl.operations.GetOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperationFactory;
import com.hazelcast.dictionary.impl.operations.PrepareAggregationOperation;
import com.hazelcast.dictionary.impl.operations.PrepareAggregationOperationFactory;
import com.hazelcast.dictionary.impl.operations.PutOperation;
import com.hazelcast.dictionary.impl.operations.RemoveAllOperation;
import com.hazelcast.dictionary.impl.operations.RemoveAllOperationFactory;
import com.hazelcast.dictionary.impl.operations.RemoveOperation;
import com.hazelcast.dictionary.impl.operations.SizeOperation;
import com.hazelcast.dictionary.impl.operations.SizeOperationFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DICTIONARY_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DICTIONARY_DS_FACTORY_ID;

public class DictionaryDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DICTIONARY_DS_FACTORY, DICTIONARY_DS_FACTORY_ID);

    public static final int GET_OPERATION = 0;
    public static final int PUT_OPERATION = 1;
    public static final int REMOVE_OPERATION = 2;
    public static final int SIZE_OPERATION = 3;
    public static final int SIZE_OPERATION_FACTORY = 4;
    public static final int REMOVE_ALL_OPERATION = 5;
    public static final int REMOVE_ALL_OPERATION_FACTORY = 6;
    public static final int MEMORY_INFO_OPERATION = 7;
    public static final int MEMORY_INFO_OPERATION_FACTORY = 8;
    public static final int PREPARE_AGGREGATION_OPERATION = 9;
    public static final int PREPARE_AGGREGATION_OPERATION_FACTORY = 10;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case GET_OPERATION:
                    return new GetOperation();
                case PUT_OPERATION:
                    return new PutOperation();
                case REMOVE_OPERATION:
                    return new RemoveOperation();
                case REMOVE_ALL_OPERATION:
                    return new RemoveAllOperation();
                case REMOVE_ALL_OPERATION_FACTORY:
                    return new RemoveAllOperationFactory();
                case SIZE_OPERATION:
                    return new SizeOperation();
                case SIZE_OPERATION_FACTORY:
                    return new SizeOperationFactory();
                case MEMORY_INFO_OPERATION:
                    return new MemoryInfoOperation();
                case MEMORY_INFO_OPERATION_FACTORY:
                    return new MemoryInfoOperationFactory();
                case PREPARE_AGGREGATION_OPERATION:
                    return new PrepareAggregationOperation();
                case PREPARE_AGGREGATION_OPERATION_FACTORY:
                    return new PrepareAggregationOperationFactory();
                default:
                    return null;
            }
        };
    }
}
