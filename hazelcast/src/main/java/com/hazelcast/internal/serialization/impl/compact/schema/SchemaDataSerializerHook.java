/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper.Factory;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.DataSerializableFactory;

/**
 * Serialization hook for the Schema and operations related to it.
 */
public class SchemaDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = Factory.SCHEMA_DS.getFactoryId();

    public static final int SCHEMA = 1;
    public static final int SEND_SCHEMA_REPLICATIONS_OPERATION = 2;
    public static final int PREPARE_SCHEMA_REPLICATION_OPERATION = 3;
    public static final int ACK_SCHEMA_REPLICATION_OPERATION = 4;
    public static final int SCHEMA_REPLICATION = 5;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case SCHEMA:
                    return new Schema();
                case SEND_SCHEMA_REPLICATIONS_OPERATION:
                    return new SendSchemaReplicationsOperation();
                case PREPARE_SCHEMA_REPLICATION_OPERATION:
                    return new PrepareSchemaReplicationOperation();
                case ACK_SCHEMA_REPLICATION_OPERATION:
                    return new AckSchemaReplicationOperation();
                case SCHEMA_REPLICATION:
                    return new SchemaReplication();
                default:
                    return null;
            }
        };
    }

}
