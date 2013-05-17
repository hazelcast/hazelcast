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

import com.hazelcast.nio.serialization.*;
import com.hazelcast.spi.impl.EventServiceImpl.EventPacket;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.ConstructorFunction;

/**
 * @mdogan 8/24/12
 */
public final class SpiDataSerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SPI_DS_FACTORY, -1);

    static final int RESPONSE = 0;
    static final int BACKUP = 1;
    static final int BACKUP_RESPONSE = 2;
    static final int PARTITION_ITERATOR = 3;
    static final int PARTITION_RESPONSE = 4;
    static final int PARALLEL_OPERATION_FACTORY = 5;
    static final int EVENT_PACKET = 6;
    static final int COLLECTION = 7;

    private static final int LEN = 10;

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[RESPONSE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ResponseOperation();
            }
        };

        constructors[BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Backup();
            }
        };

        constructors[BACKUP_RESPONSE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BackupResponse();
            }
        };

        constructors[EVENT_PACKET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EventPacket();
            }
        };

        constructors[PARTITION_ITERATOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionIteratingOperation();
            }
        };

        constructors[PARTITION_RESPONSE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitionResponse();
            }
        };

        constructors[PARALLEL_OPERATION_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BinaryOperationFactory();
            }
        };

        constructors[COLLECTION] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SerializableCollection();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }

    public int getFactoryId() {
        return F_ID;
    }
}
