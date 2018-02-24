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

package com.hazelcast.jet.core;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.jet.impl.SerializationConstants;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ApplyFnEntryProcessor;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.jet.impl.SerializationConstants.DAG;
import static com.hazelcast.jet.impl.SerializationConstants.EDGE;
import static com.hazelcast.jet.impl.SerializationConstants.APPLY_FN_ENTRY_PROCESSOR;
import static com.hazelcast.jet.impl.SerializationConstants.VERTEX;

/**
 * A Java Service Provider hook for Hazelcast's Identified Data Serializable
 * mechanism.
 */
public final class JetDataSerializerHook implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return SerializationConstants.FACTORY_ID;
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
                case EDGE:
                    return new Edge();
                case VERTEX:
                    return new Vertex();
                case APPLY_FN_ENTRY_PROCESSOR:
                    return new ApplyFnEntryProcessor();
                default:
                    throw new IllegalArgumentException("Unknown type id " + typeId);
            }
        }
    }
}
