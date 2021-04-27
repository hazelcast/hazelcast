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

package com.hazelcast.internal.serialization.impl.compact;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CompactTestUtil {

    private CompactTestUtil() {

    }

    public static SchemaService createInMemorySchemaService() {
        return new SchemaService() {
            private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();

            @Override
            public Schema get(long schemaId) {
                return schemas.get(schemaId);
            }

            @Override
            public void put(Schema schema) {
                long schemaId = schema.getSchemaId();
                Schema existingSchema = schemas.putIfAbsent(schemaId, schema);
                if (existingSchema != null && !schema.equals(existingSchema)) {
                    throw new IllegalStateException("Schema with schemaId " + schemaId + " already exists. "
                            + "existing schema " + existingSchema
                            + "new schema " + schema);
                }
            }
        };
    }
}
