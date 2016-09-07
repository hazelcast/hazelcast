/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cardinality;

import com.hazelcast.cardinality.operations.AggregateAndEstimateOperation;
import com.hazelcast.cardinality.operations.AggregateOperation;
import com.hazelcast.cardinality.operations.BatchAggregateAndEstimateOperation;
import com.hazelcast.cardinality.operations.BatchAggregateOperation;
import com.hazelcast.cardinality.operations.EstimateOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CARDINALITY_ESTIMATOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CARDINALITY_ESTIMATOR_DS_FACTORY_ID;

public class CardinalityEstimatorDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CARDINALITY_ESTIMATOR_DS_FACTORY,
            CARDINALITY_ESTIMATOR_DS_FACTORY_ID);

    public static final int AGGREGATE = 0;
    public static final int BATCH_AGGREGATE = 1;
    public static final int EST_CARDINALITY = 2;
    public static final int AGGREGATE_AND_ESTIMATE = 3;
    public static final int BATCH_AGGREGATE_AND_ESTIMATE = 4;


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
                    case AGGREGATE:
                        return new AggregateOperation();
                    case BATCH_AGGREGATE:
                        return new BatchAggregateOperation();
                    case EST_CARDINALITY:
                        return new EstimateOperation();
                    case AGGREGATE_AND_ESTIMATE:
                        return new AggregateAndEstimateOperation();
                    case BATCH_AGGREGATE_AND_ESTIMATE:
                        return new BatchAggregateAndEstimateOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
