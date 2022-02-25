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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.hyperloglog.impl.DenseHyperLogLogEncoder;
import com.hazelcast.cardinality.impl.hyperloglog.impl.HyperLogLogImpl;
import com.hazelcast.cardinality.impl.hyperloglog.impl.SparseHyperLogLogEncoder;
import com.hazelcast.cardinality.impl.operations.AggregateBackupOperation;
import com.hazelcast.cardinality.impl.operations.AggregateOperation;
import com.hazelcast.cardinality.impl.operations.EstimateOperation;
import com.hazelcast.cardinality.impl.operations.MergeBackupOperation;
import com.hazelcast.cardinality.impl.operations.MergeOperation;
import com.hazelcast.cardinality.impl.operations.ReplicationOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CARDINALITY_ESTIMATOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CARDINALITY_ESTIMATOR_DS_FACTORY_ID;

public final class CardinalityEstimatorDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CARDINALITY_ESTIMATOR_DS_FACTORY,
            CARDINALITY_ESTIMATOR_DS_FACTORY_ID);

    public static final int ADD = 0;
    public static final int ESTIMATE = 1;
    public static final int AGGREGATE_BACKUP = 2;
    public static final int REPLICATION = 3;
    public static final int CARDINALITY_EST_CONTAINER = 4;
    public static final int HLL = 5;
    public static final int HLL_DENSE_ENC = 6;
    public static final int HLL_SPARSE_ENC = 7;
    public static final int MERGE = 8;
    public static final int MERGE_BACKUP = 9;

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
                    case ADD:
                        return new AggregateOperation();
                    case ESTIMATE:
                        return new EstimateOperation();
                    case AGGREGATE_BACKUP:
                        return new AggregateBackupOperation();
                    case REPLICATION:
                        return new ReplicationOperation();
                    case CARDINALITY_EST_CONTAINER:
                        return new CardinalityEstimatorContainer();
                    case HLL:
                        return new HyperLogLogImpl();
                    case HLL_DENSE_ENC:
                        return new DenseHyperLogLogEncoder();
                    case HLL_SPARSE_ENC:
                        return new SparseHyperLogLogEncoder();
                    case MERGE:
                        return new MergeOperation();
                    case MERGE_BACKUP:
                        return new MergeBackupOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
