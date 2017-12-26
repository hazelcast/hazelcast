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

package com.hazelcast.dataset.impl;

import com.hazelcast.dataset.impl.aggregation.AggregateOperation;
import com.hazelcast.dataset.impl.aggregation.AggregateOperationFactory;
import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperation;
import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperationFactory;
import com.hazelcast.dataset.impl.aggregation.FetchAggregateOperation;
import com.hazelcast.dataset.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.dataset.impl.entryprocessor.CompileEpOperation;
import com.hazelcast.dataset.impl.entryprocessor.CompileEpOperationFactory;
import com.hazelcast.dataset.impl.entryprocessor.EpOperation;
import com.hazelcast.dataset.impl.entryprocessor.EpOperationFactory;
import com.hazelcast.dataset.impl.operations.CountOperation;
import com.hazelcast.dataset.impl.operations.CountOperationFactory;
import com.hazelcast.dataset.impl.operations.InsertOperation;
import com.hazelcast.dataset.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataset.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataset.impl.operations.PopulateOperation;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperation;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperationFactory;
import com.hazelcast.dataset.impl.projection.NewDataSetOperation;
import com.hazelcast.dataset.impl.projection.NewDataSetOperationFactory;
import com.hazelcast.dataset.impl.projection.ProjectionOperation;
import com.hazelcast.dataset.impl.projection.ProjectionOperationFactory;
import com.hazelcast.dataset.impl.query.CompilePredicateOperation;
import com.hazelcast.dataset.impl.query.CompilePredicateOperationFactory;
import com.hazelcast.dataset.impl.query.QueryOperationFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY_ID;

public final class DataSetDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DATA_SET_DS_FACTORY, DATA_SET_DS_FACTORY_ID);

    public static final int INSERT_OPERATION = 0;
    public static final int COMPILE_PREDICATE_OPERATION = 1;
    public static final int COMPILE_PREDICATE_OPERATION_FACTORY = 2;
    public static final int QUERY_OPERATION = 3;
    public static final int QUERY_OPERATION_FACTORY = 4;
    public static final int COUNT_OPERATION = 5;
    public static final int COUNT_OPERATION_FACTORY = 6;
    public static final int COMPILE_PROJECTION_OPERATION = 7;
    public static final int COMPILE_PROJECTION_OPERATION_FACTORY = 8;
    public static final int AGGREGATE_OPERATION = 9;
    public static final int AGGREGATE_OPERATION_FACTORY = 10;
    public static final int COMPILE_AGGREGATION = 11;
    public static final int COMPILE_AGGREGATION_OPERATION_FACTORY = 12;
    public static final int MEMORY_USAGE_OPERATION = 13;
    public static final int MEMORY_USAGE_OPERATION_FACTORY = 14;
    public static final int NEW_DATASET_OPERATION = 15;
    public static final int NEW_DATASET_OPERATION_FACTORY = 16;
    public static final int POPULATE_OPERATION = 17;
    public static final int POPULATE_OPERATION_FACTORY = 18;
    public static final int COMPILE_ENTRY_PROCESSOR_OPERATION = 19;
    public static final int COMPILE_ENTRY_PROCESSOR_OPERATION_FACTORY = 20;
    public static final int ENTRY_PROCESSOR_OPERATION = 21;
    public static final int ENTRY_PROCESSOR_OPERATION_FACTORY = 22;
    public static final int FETCH_AGGREGATOR_OPERATION = 23;
    public static final int FETCH_AGGREGATOR_OPERATION_FACTORY = 24;
    public static final int PROJECTION_OPERATION = 25;
    public static final int PROJECTION_OPERATION_FACTORY = 26;


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
                    case INSERT_OPERATION:
                        return new InsertOperation();
                    case COMPILE_PREDICATE_OPERATION:
                        return new CompilePredicateOperation();
                    case COMPILE_PREDICATE_OPERATION_FACTORY:
                        return new CompilePredicateOperationFactory();
                    case QUERY_OPERATION:
                        return new QueryOperation();
                    case QUERY_OPERATION_FACTORY:
                        return new QueryOperationFactory();
                    case COUNT_OPERATION:
                        return new CountOperation();
                    case COUNT_OPERATION_FACTORY:
                        return new CountOperationFactory();
                    case COMPILE_PROJECTION_OPERATION:
                        return new CompileProjectionOperation();
                    case COMPILE_PROJECTION_OPERATION_FACTORY:
                        return new CompileProjectionOperationFactory();
                    case AGGREGATE_OPERATION:
                        return new AggregateOperation();
                    case AGGREGATE_OPERATION_FACTORY:
                        return new AggregateOperationFactory();
                    case COMPILE_AGGREGATION:
                        return new CompileAggregationOperation();
                    case COMPILE_AGGREGATION_OPERATION_FACTORY:
                        return new CompileAggregationOperationFactory();
                    case MEMORY_USAGE_OPERATION:
                        return new MemoryUsageOperation();
                    case MEMORY_USAGE_OPERATION_FACTORY:
                        return new MemoryUsageOperationFactory();
                    case NEW_DATASET_OPERATION:
                        return new NewDataSetOperation();
                    case NEW_DATASET_OPERATION_FACTORY:
                        return new NewDataSetOperationFactory();
                    case POPULATE_OPERATION:
                        return new PopulateOperation();
                    case COMPILE_ENTRY_PROCESSOR_OPERATION:
                        return new CompileEpOperation();
                    case COMPILE_ENTRY_PROCESSOR_OPERATION_FACTORY:
                        return new CompileEpOperationFactory();
                    case ENTRY_PROCESSOR_OPERATION:
                        return new EpOperation();
                    case ENTRY_PROCESSOR_OPERATION_FACTORY:
                        return new EpOperationFactory();
                    case FETCH_AGGREGATOR_OPERATION:
                        return new FetchAggregateOperation();
                    case FETCH_AGGREGATOR_OPERATION_FACTORY:
                        return new FetchAggregateOperationFactory();
                    case PROJECTION_OPERATION:
                        return new ProjectionOperation();
                    case PROJECTION_OPERATION_FACTORY:
                        return new ProjectionOperationFactory();
                    default:
                        return null;
                }
            }
        };
    }
}
