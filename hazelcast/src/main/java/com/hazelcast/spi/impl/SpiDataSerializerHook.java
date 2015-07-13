/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.eventservice.impl.EventPacket;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import static com.hazelcast.nio.serialization.impl.FactoryIdHelper.SPI_DS_FACTORY;
import static com.hazelcast.nio.serialization.impl.FactoryIdHelper.SPI_DS_FACTORY_ID;

public final class SpiDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SPI_DS_FACTORY, SPI_DS_FACTORY_ID);

    public static final int NORMAL_RESPONSE = 0;
    public static final int BACKUP = 1;
    public static final int BACKUP_RESPONSE = 2;
    public static final int PARTITION_ITERATOR = 3;
    public static final int PARTITION_RESPONSE = 4;
    public static final int PARALLEL_OPERATION_FACTORY = 5;
    public static final int EVENT_PACKET = 6;
    public static final int COLLECTION = 7;
    public static final int CALL_TIMEOUT_RESPONSE = 8;
    public static final int ERROR_RESPONSE = 9;

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case NORMAL_RESPONSE:
                        return new NormalResponse();
                    case BACKUP:
                        return new Backup();
                    case BACKUP_RESPONSE:
                        return new BackupResponse();
                    case PARTITION_ITERATOR:
                        return new PartitionIteratingOperation();
                    case PARTITION_RESPONSE:
                        return new PartitionResponse();
                    case PARALLEL_OPERATION_FACTORY:
                        return new BinaryOperationFactory();
                    case EVENT_PACKET:
                        return new EventPacket();
                    case COLLECTION:
                        return new SerializableList();
                    case CALL_TIMEOUT_RESPONSE:
                        return new CallTimeoutResponse();
                    case ERROR_RESPONSE:
                        return new ErrorResponse();
                    default:
                        return null;
                }
            }
        };
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }
}
