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

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.EventServiceImpl.EventPacket;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;

public final class SpiDataSerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.SPI_DS_FACTORY, -1);

    static final int NORMAL_RESPONSE = 0;
    static final int BACKUP = 1;
    static final int BACKUP_RESPONSE = 2;
    static final int PARTITION_ITERATOR = 3;
    static final int PARTITION_RESPONSE = 4;
    static final int PARALLEL_OPERATION_FACTORY = 5;
    static final int EVENT_PACKET = 6;
    static final int COLLECTION = 7;

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
                        return new SerializableCollection();
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
