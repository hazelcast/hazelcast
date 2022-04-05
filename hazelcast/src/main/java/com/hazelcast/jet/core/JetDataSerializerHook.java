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

package com.hazelcast.jet.core;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.impl.connector.AbstractUpdateMapP.ApplyValuesEntryProcessor;
import com.hazelcast.jet.impl.connector.HazelcastReaders;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP;
import com.hazelcast.jet.impl.connector.UpdateMapP.ApplyFnEntryProcessor;
import com.hazelcast.jet.impl.processor.ProcessorSuppliers;
import com.hazelcast.jet.pipeline.test.impl.ItemsDistributedFillBufferFn;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.sql.impl.row.JetSqlRow;

import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_DS_FACTORY;
import static com.hazelcast.jet.impl.JetFactoryIdHelper.JET_DS_FACTORY_ID;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable
 * mechanism. This is private API.
 */
@PrivateApi
@SuppressWarnings("checkstyle:JavadocVariable")
public final class JetDataSerializerHook implements DataSerializerHook {

    public static final int DAG = 0;
    public static final int VERTEX = 1;
    public static final int EDGE = 2;
    public static final int APPLY_FN_ENTRY_PROCESSOR = 3;
    public static final int APPLY_VALUE_ENTRY_PROCESSOR = 4;
    public static final int TEST_SOURCES_ITEMS_DISTRIBUTED_FILL_BUFFER_FN = 5;
    public static final int JET_SQL_ROW = 6;
    public static final int LOCAL_MAP_READER_FUNCTION = 7;
    public static final int PROCESSORS_AGGREGATE_P_SUPPLIER = 8;
    public static final int LOCAL_CACHE_READER_FUNCTION = 9;
    public static final int REMOTE_CACHE_READER_FUNCTION = 10;
    public static final int LOCAL_MAP_QUERY_READER_FUNCTION = 11;
    public static final int REMOTE_MAP_READER_FUNCTION = 12;
    public static final int REMOTE_MAP_QUERY_READER_FUNCTION = 13;
    public static final int READ_MAP_OR_CACHE_P_LOCAL_PROCESSOR_SUPPLIER = 14;
    public static final int PROCESSOR_MAP_P_SUPPLIER = 15;
    public static final int AGGREGATE_COMBINING_ACCUMULATE = 16;
    public static final int EDGE_KEY_PARTITIONER = 17;
    public static final int EDGE_SINGLE_PARTITIONER = 18;
    public static final int EXPECT_NOTHING_PROCESSOR_SUPPLIER = 19;
    public static final int SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER = 20;

    /**
     * Factory ID
     */
    public static final int FACTORY_ID = FactoryIdHelper.getFactoryId(JET_DS_FACTORY, JET_DS_FACTORY_ID);

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            switch (typeId) {
                case DAG:
                    return new DAG();
                case VERTEX:
                    return new Vertex();
                case EDGE:
                    return new Edge();
                case APPLY_FN_ENTRY_PROCESSOR:
                    return new ApplyFnEntryProcessor<>();
                case APPLY_VALUE_ENTRY_PROCESSOR:
                    return new ApplyValuesEntryProcessor<>();
                case TEST_SOURCES_ITEMS_DISTRIBUTED_FILL_BUFFER_FN:
                    return new ItemsDistributedFillBufferFn<>();
                case JET_SQL_ROW:
                    return new JetSqlRow();
                case LOCAL_MAP_READER_FUNCTION:
                    return new HazelcastReaders.LocalMapReaderFunction();
                case PROCESSORS_AGGREGATE_P_SUPPLIER:
                    return new ProcessorSuppliers.AggregatePSupplier<>();
                case LOCAL_CACHE_READER_FUNCTION:
                    return new HazelcastReaders.LocalCacheReaderFunction();
                case REMOTE_CACHE_READER_FUNCTION:
                    return new HazelcastReaders.RemoteCacheReaderFunction();
                case LOCAL_MAP_QUERY_READER_FUNCTION:
                    return new HazelcastReaders.LocalMapQueryReaderFunction<>();
                case REMOTE_MAP_READER_FUNCTION:
                    return new HazelcastReaders.RemoteMapReaderFunction();
                case REMOTE_MAP_QUERY_READER_FUNCTION:
                    return new HazelcastReaders.RemoteMapQueryReaderFunction<>();
                case READ_MAP_OR_CACHE_P_LOCAL_PROCESSOR_SUPPLIER:
                    return new ReadMapOrCacheP.LocalProcessorSupplier<>();
                case PROCESSOR_MAP_P_SUPPLIER:
                    return new ProcessorSuppliers.ProcessorMapPSupplier<>();
                case AGGREGATE_COMBINING_ACCUMULATE:
                    return new AggregateOperation1Impl.AggregateCombiningAccumulate<>();
                case EDGE_KEY_PARTITIONER:
                    return new Edge.KeyPartitioner<>();
                case EDGE_SINGLE_PARTITIONER:
                    return new Edge.Single();
                case EXPECT_NOTHING_PROCESSOR_SUPPLIER:
                    return new ProcessorMetaSupplier.ExpectNothingProcessorSupplier();
                case SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER:
                    return new ProcessorMetaSupplier.SpecificMemberPms();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
