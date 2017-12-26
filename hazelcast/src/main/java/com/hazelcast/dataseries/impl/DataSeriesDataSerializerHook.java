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

package com.hazelcast.dataseries.impl;

import com.hazelcast.dataseries.impl.operations.AppendOperation;
import com.hazelcast.dataseries.impl.operations.CountOperation;
import com.hazelcast.dataseries.impl.operations.CountOperationFactory;
import com.hazelcast.dataseries.impl.operations.FillOperation;
import com.hazelcast.dataseries.impl.operations.IteratorOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataseries.impl.operations.PopulateOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY_ID;

public final class DataSeriesDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DATA_SET_DS_FACTORY, DATA_SET_DS_FACTORY_ID);

    public static final int APPEND_OPERATION = 0;
    public static final int COUNT_OPERATION = 1;
    public static final int COUNT_OPERATION_FACTORY = 2;
    public static final int MEMORY_USAGE_OPERATION = 3;
    public static final int MEMORY_USAGE_OPERATION_FACTORY = 4;
    public static final int POPULATE_OPERATION = 5;
    public static final int POPULATE_OPERATION_FACTORY = 6;
    public static final int FILL_OPERATION = 7;
    public static final int ITERATOR_OPERATION = 8;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case APPEND_OPERATION:
                        return new AppendOperation();
                    case COUNT_OPERATION:
                        return new CountOperation();
                    case COUNT_OPERATION_FACTORY:
                        return new CountOperationFactory();
                    case MEMORY_USAGE_OPERATION:
                        return new MemoryUsageOperation();
                    case MEMORY_USAGE_OPERATION_FACTORY:
                        return new MemoryUsageOperationFactory();
                    case POPULATE_OPERATION:
                        return new PopulateOperation();
                    case FILL_OPERATION:
                        return new FillOperation();
                    case ITERATOR_OPERATION:
                        return new IteratorOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
