/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.PortableReader;

import java.io.IOException;

/**
 * Utility class to extract a single field from a {@link com.hazelcast.nio.serialization.Portable} binary.
 */
final class PortableExtractor {

    private PortableExtractor() {
    }

    public static Object extractValue(InternalSerializationService serializationService, Data data, String fieldName)
            throws IOException {
        PortableContext context = serializationService.getPortableContext();
        PortableReader reader = serializationService.createPortableReader(data);
        ClassDefinition classDefinition = context.lookupClassDefinition(data);
        FieldDefinition fieldDefinition = context.getFieldDefinition(classDefinition, fieldName);

        if (fieldDefinition != null) {
            return reader.read(fieldName);
        } else {
            return null;
        }
    }

}
