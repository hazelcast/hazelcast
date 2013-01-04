/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.spi.impl.EventServiceImpl.EventPacket;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 8/24/12
 */
public final class DataSerializerInitHook implements DataSerializerHook {

    static final int RESPONSE = 1;
    static final int MULTI_RESPONSE = 2;
    static final int PARTITION_ITERATOR = 3;
    static final int PARTITION_RESPONSE = 4;
    static final int OPERATION_WRAPPER = 5;
    static final int EVENT_PACKET = 6;

    public Map<Integer, DataSerializableFactory> getFactories() {
        final Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();

        factories.put(RESPONSE, new DataSerializableFactory() {
            public DataSerializable create() {
                return new Response();
            }
        });

        factories.put(MULTI_RESPONSE, new DataSerializableFactory() {
            public DataSerializable create() {
                return new MultiResponse();
            }
        });

        factories.put(EVENT_PACKET, new DataSerializableFactory() {
            public DataSerializable create() {
                return new EventPacket();
            }
        });

        factories.put(PARTITION_ITERATOR, new DataSerializableFactory() {
            public DataSerializable create() {
                return new PartitionIteratingOperation();
            }
        });

        factories.put(PARTITION_RESPONSE, new DataSerializableFactory() {
            public DataSerializable create() {
                return new PartitionResponse();
            }
        });

        factories.put(OPERATION_WRAPPER, new DataSerializableFactory() {
            public DataSerializable create() {
                return new OperationWrapper();
            }
        });

        return factories;
    }
}
