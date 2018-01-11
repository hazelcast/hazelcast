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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.OperationControl;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.impl.eventservice.impl.EventEnvelope;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.impl.eventservice.impl.operations.DeregistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.OnJoinRegistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.RegistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.SendEventOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.proxyservice.impl.operations.DistributedObjectDestroyOperation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.InitializeDistributedObjectOperation;
import com.hazelcast.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPI_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SPI_DS_FACTORY_ID;

public final class SpiDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SPI_DS_FACTORY, SPI_DS_FACTORY_ID);

    public static final int NORMAL_RESPONSE = 0;
    public static final int BACKUP = 1;
    public static final int BACKUP_ACK_RESPONSE = 2;
    public static final int PARTITION_ITERATOR = 3;
    public static final int PARTITION_RESPONSE = 4;
    public static final int PARALLEL_OPERATION_FACTORY = 5;
    public static final int EVENT_ENVELOPE = 6;
    public static final int COLLECTION = 7;
    public static final int CALL_TIMEOUT_RESPONSE = 8;
    public static final int ERROR_RESPONSE = 9;
    public static final int DEREGISTRATION = 10;
    public static final int ON_JOIN_REGISTRATION = 11;
    public static final int REGISTRATION = 12;
    public static final int SEND_EVENT = 13;
    public static final int DIST_OBJECT_INIT = 14;
    public static final int DIST_OBJECT_DESTROY = 15;
    public static final int POST_JOIN_PROXY = 16;
    public static final int TRUE_EVENT_FILTER = 17;
    public static final int UNMODIFIABLE_LAZY_LIST = 18;
    public static final int OPERATION_CONTROL = 19;
    public static final int DISTRIBUTED_OBJECT_NS = 20;

    private static final DataSerializableFactory FACTORY = createFactoryInternal();

    @Override
    public DataSerializableFactory createFactory() {
        return FACTORY;
    }

    private static DataSerializableFactory createFactoryInternal() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case NORMAL_RESPONSE:
                        return new NormalResponse();
                    case BACKUP:
                        return new Backup();
                    case BACKUP_ACK_RESPONSE:
                        return new BackupAckResponse();
                    case PARTITION_ITERATOR:
                        return new PartitionIteratingOperation();
                    case PARTITION_RESPONSE:
                        return new PartitionResponse();
                    case PARALLEL_OPERATION_FACTORY:
                        return new BinaryOperationFactory();
                    case EVENT_ENVELOPE:
                        return new EventEnvelope();
                    case COLLECTION:
                        return new SerializableList();
                    case CALL_TIMEOUT_RESPONSE:
                        return new CallTimeoutResponse();
                    case ERROR_RESPONSE:
                        return new ErrorResponse();
                    case DEREGISTRATION:
                        return new DeregistrationOperation();
                    case ON_JOIN_REGISTRATION:
                        return new OnJoinRegistrationOperation();
                    case REGISTRATION:
                        return new RegistrationOperation();
                    case SEND_EVENT:
                        return new SendEventOperation();
                    case DIST_OBJECT_INIT:
                        return new InitializeDistributedObjectOperation();
                    case DIST_OBJECT_DESTROY:
                        return new DistributedObjectDestroyOperation();
                    case POST_JOIN_PROXY:
                        return new PostJoinProxyOperation();
                    case TRUE_EVENT_FILTER:
                        return new TrueEventFilter();
                    case UNMODIFIABLE_LAZY_LIST:
                        return new UnmodifiableLazyList();
                    case OPERATION_CONTROL:
                        return new OperationControl();
                    case DISTRIBUTED_OBJECT_NS:
                        return new DistributedObjectNamespace();
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
