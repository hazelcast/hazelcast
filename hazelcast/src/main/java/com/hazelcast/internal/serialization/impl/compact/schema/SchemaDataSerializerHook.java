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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.DataSerializableFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEMA_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEMA_DS_FACTORY_ID;

/**
 * Serialization hook for the Schema and operations related to it.
 */
public class SchemaDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SCHEMA_DS_FACTORY, SCHEMA_DS_FACTORY_ID);

    public static final int SCHEMA = 1;
    public static final int SEND_SCHEMA_OPERATION = 2;
    public static final int FETCH_SCHEMA_OPERATION = 3;
    public static final int SEND_ALL_SCHEMAS_OPERATION = 4;

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
                case SEND_SCHEMA_OPERATION:
                    return new SendSchemaOperation();
                case FETCH_SCHEMA_OPERATION:
                    return new FetchSchemaOperation();
                case SEND_ALL_SCHEMAS_OPERATION:
                    return new SendAllSchemasOperation();
                default:
                    return null;
            }
        };
    }

}
