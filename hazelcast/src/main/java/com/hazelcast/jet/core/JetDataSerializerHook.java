/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.connector.AbstractUpdateMapP.ApplyValuesEntryProcessor;
import com.hazelcast.jet.impl.connector.UpdateMapP.ApplyFnEntryProcessor;
import com.hazelcast.jet.pipeline.test.impl.ItemsDistributedFillBufferFn;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

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
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
