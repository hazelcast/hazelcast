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

import com.hazelcast.dataseries.impl.aggregation.ExecuteAggregationOperation;
import com.hazelcast.dataseries.impl.aggregation.ExecuteAggregationOperationFactory;
import com.hazelcast.dataseries.impl.aggregation.FetchAggregateOperation;
import com.hazelcast.dataseries.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.dataseries.impl.aggregation.PrepareAggregationOperation;
import com.hazelcast.dataseries.impl.aggregation.PrepareAggregationOperationFactory;
import com.hazelcast.dataseries.impl.entryprocessor.ExecuteEntryProcessorOperation;
import com.hazelcast.dataseries.impl.entryprocessor.ExecuteEntryProcessorOperationFactory;
import com.hazelcast.dataseries.impl.entryprocessor.PrepareEntryProcessorOperation;
import com.hazelcast.dataseries.impl.entryprocessor.PrepareEntryProcessorOperationFactory;
import com.hazelcast.dataseries.impl.operations.CountOperation;
import com.hazelcast.dataseries.impl.operations.CountOperationFactory;
import com.hazelcast.dataseries.impl.operations.FillOperation;
import com.hazelcast.dataseries.impl.operations.FreezeOperation;
import com.hazelcast.dataseries.impl.operations.FreezeOperationFactory;
import com.hazelcast.dataseries.impl.operations.InsertOperation;
import com.hazelcast.dataseries.impl.operations.IteratorOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataseries.impl.operations.PopulateOperation;
import com.hazelcast.dataseries.impl.projection.ExecuteProjectionOperation;
import com.hazelcast.dataseries.impl.projection.ExecuteProjectionOperationFactory;
import com.hazelcast.dataseries.impl.projection.NewDataSeriesOperation;
import com.hazelcast.dataseries.impl.projection.NewDataSeriesOperationFactory;
import com.hazelcast.dataseries.impl.projection.PrepareProjectionOperation;
import com.hazelcast.dataseries.impl.projection.PrepareProjectionOperationFactory;
import com.hazelcast.dataseries.impl.query.ExecuteQueryOperationFactory;
import com.hazelcast.dataseries.impl.query.PrepareQueryOperation;
import com.hazelcast.dataseries.impl.query.PrepareQueryOperationFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY_ID;

public final class DataSeriesDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DATA_SET_DS_FACTORY, DATA_SET_DS_FACTORY_ID);

    public static final int INSERT_OPERATION = 0;
    public static final int PREPARE_QUERY_OPERATION = 1;
    public static final int PREPARE_QUERY_OPERATION_FACTORY = 2;
    public static final int EXECUTE_QUERY_OPERATION = 3;
    public static final int EXECUTE_QUERY_OPERATION_FACTORY = 4;
    public static final int COUNT_OPERATION = 5;
    public static final int COUNT_OPERATION_FACTORY = 6;
    public static final int PREPARE_PROJECTION_OPERATION = 7;
    public static final int PREPARE_PROJECTION_OPERATION_FACTORY = 8;
    public static final int EXECUTE_AGGREGATION_OPERATION = 9;
    public static final int EXECUTE_AGGREGATION_OPERATION_FACTORY = 10;
    public static final int PREPARE_AGGREGATION = 11;
    public static final int PREPARE_AGGREGATION_OPERATION_FACTORY = 12;
    public static final int MEMORY_USAGE_OPERATION = 13;
    public static final int MEMORY_USAGE_OPERATION_FACTORY = 14;
    public static final int NEW_DATASERIES_OPERATION = 15;
    public static final int NEW_DATASERIES_OPERATION_FACTORY = 16;
    public static final int POPULATE_OPERATION = 17;
    public static final int POPULATE_OPERATION_FACTORY = 18;
    public static final int PREPARE_ENTRY_PROCESSOR_OPERATION = 19;
    public static final int PREPARE_ENTRY_PROCESSOR_OPERATION_FACTORY = 20;
    public static final int EXECUTE_ENTRY_PROCESSOR_OPERATION = 21;
    public static final int EXECUTE_ENTRY_PROCESSOR_OPERATION_FACTORY = 22;
    public static final int FETCH_AGGREGATOR_OPERATION = 23;
    public static final int FETCH_AGGREGATOR_OPERATION_FACTORY = 24;
    public static final int EXECUTE_PROJECTION_OPERATION = 25;
    public static final int EXECUTE_PROJECTION_OPERATION_FACTORY = 26;
    public static final int FREEZE_OPERATION = 27;
    public static final int FREEZE_OPERATION_FACTORY = 28;
    public static final int FILL_OPERATION = 29;
    public static final int ITERATOR_OPERATION = 30;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case INSERT_OPERATION:
                    return new InsertOperation();
                case PREPARE_QUERY_OPERATION:
                    return new PrepareQueryOperation();
                case PREPARE_QUERY_OPERATION_FACTORY:
                    return new PrepareQueryOperationFactory();
                case EXECUTE_QUERY_OPERATION:
                    return new QueryOperation();
                case EXECUTE_QUERY_OPERATION_FACTORY:
                    return new ExecuteQueryOperationFactory();
                case COUNT_OPERATION:
                    return new CountOperation();
                case COUNT_OPERATION_FACTORY:
                    return new CountOperationFactory();
                case PREPARE_PROJECTION_OPERATION:
                    return new PrepareProjectionOperation();
                case PREPARE_PROJECTION_OPERATION_FACTORY:
                    return new PrepareProjectionOperationFactory();
                case EXECUTE_AGGREGATION_OPERATION:
                    return new ExecuteAggregationOperation();
                case EXECUTE_AGGREGATION_OPERATION_FACTORY:
                    return new ExecuteAggregationOperationFactory();
                case PREPARE_AGGREGATION:
                    return new PrepareAggregationOperation();
                case PREPARE_AGGREGATION_OPERATION_FACTORY:
                    return new PrepareAggregationOperationFactory();
                case MEMORY_USAGE_OPERATION:
                    return new MemoryUsageOperation();
                case MEMORY_USAGE_OPERATION_FACTORY:
                    return new MemoryUsageOperationFactory();
                case NEW_DATASERIES_OPERATION:
                    return new NewDataSeriesOperation();
                case NEW_DATASERIES_OPERATION_FACTORY:
                    return new NewDataSeriesOperationFactory();
                case POPULATE_OPERATION:
                    return new PopulateOperation();
                case PREPARE_ENTRY_PROCESSOR_OPERATION:
                    return new PrepareEntryProcessorOperation();
                case PREPARE_ENTRY_PROCESSOR_OPERATION_FACTORY:
                    return new PrepareEntryProcessorOperationFactory();
                case EXECUTE_ENTRY_PROCESSOR_OPERATION:
                    return new ExecuteEntryProcessorOperation();
                case EXECUTE_ENTRY_PROCESSOR_OPERATION_FACTORY:
                    return new ExecuteEntryProcessorOperationFactory();
                case FETCH_AGGREGATOR_OPERATION:
                    return new FetchAggregateOperation();
                case FETCH_AGGREGATOR_OPERATION_FACTORY:
                    return new FetchAggregateOperationFactory();
                case EXECUTE_PROJECTION_OPERATION:
                    return new ExecuteProjectionOperation();
                case EXECUTE_PROJECTION_OPERATION_FACTORY:
                    return new ExecuteProjectionOperationFactory();
                case FREEZE_OPERATION_FACTORY:
                    return new FreezeOperationFactory();
                case FREEZE_OPERATION:
                    return new FreezeOperation();
                case FILL_OPERATION:
                    return new FillOperation();
                case ITERATOR_OPERATION:
                    return new IteratorOperation();
                default:
                    return null;
            }
        };
    }
}
