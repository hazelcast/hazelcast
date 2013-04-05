/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.EventServiceImpl.EventPacket;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 8/24/12
 */
public final class DataSerializerSpiHook implements DataSerializerHook {

    static final int DATA = Data.ID;
    static final int RESPONSE = 1;
    static final int PARTITION_ITERATOR = 2;
    static final int PARTITION_RESPONSE = 3;
    static final int PARALLEL_OPERATION_FACTORY = 4;
    static final int EVENT_PACKET = 5;

    public Map<Integer, DataSerializableFactory> getFactories() {
        final Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();

        factories.put(DATA, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new Data();
            }
        });

        factories.put(RESPONSE, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new Response();
            }
        });

        factories.put(EVENT_PACKET, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new EventPacket();
            }
        });

        factories.put(PARTITION_ITERATOR, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new PartitionIteratingOperation();
            }
        });

        factories.put(PARTITION_RESPONSE, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new PartitionResponse();
            }
        });

        factories.put(PARALLEL_OPERATION_FACTORY, new DataSerializableFactory() {
            public IdentifiedDataSerializable create() {
                return new ParallelOperationFactory();
            }
        });

        return factories;
    }
}
