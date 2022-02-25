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

package com.hazelcast.jet.impl.aggregate;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_AGGREGATE_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_AGGREGATE_DS_FACTORY_ID;

public class AggregateDataSerializerHook implements DataSerializerHook {


    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_AGGREGATE_DS_FACTORY,
            JET_AGGREGATE_DS_FACTORY_ID);

    /**
     * Serialization ID of the {@link AggregateOpAggregator} class.
     */
    public static final int AGGREGATE_OP_AGGREGATOR = 0;

    /**
     * Serialization ID of the {@link AggregateOperationImpl} class.
     */
    public static final int AGGREGATE_OPERATION_IMPL = 1;

    /**
     * Serialization ID of the {@link AggregateOperation1Impl} class.
     */
    public static final int AGGREGATE_OPERATION_1_IMPL = 2;

    /**
     * Serialization ID of the {@link AggregateOperation2Impl} class.
     */
    public static final int AGGREGATE_OPERATION_2_IMPL = 3;

    /**
     * Serialization ID of the {@link AggregateOperation3Impl} class.
     */
    public static final int AGGREGATE_OPERATION_3_IMPL = 4;


    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case AGGREGATE_OP_AGGREGATOR:
                    return new AggregateOpAggregator<>();
                case AGGREGATE_OPERATION_IMPL:
                    return new AggregateOperationImpl<>();
                case AGGREGATE_OPERATION_1_IMPL:
                    return new AggregateOperation1Impl<>();
                case AGGREGATE_OPERATION_2_IMPL:
                    return new AggregateOperation2Impl<>();
                case AGGREGATE_OPERATION_3_IMPL:
                    return new AggregateOperation3Impl<>();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        };
    }
}
