/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map.options;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_FACTORY_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_FACTORY_ID;
import static java.lang.String.format;

// TODO: deduplicate with MapSampleMetadataResolver
public class PortableMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String factoryId = options.get(isKey ? TO_KEY_FACTORY_ID : TO_VALUE_FACTORY_ID);
        String classId = options.get(isKey ? TO_KEY_CLASS_ID : TO_VALUE_CLASS_ID);
        String classVersion = options.get(isKey ? TO_KEY_CLASS_VERSION : TO_VALUE_CLASS_VERSION);

        if (factoryId != null && classId != null && classVersion != null) {
            ClassDefinition classDefinition = lookupClassDefinition(
                    serializationService,
                    Integer.parseInt(factoryId),
                    Integer.parseInt(classId),
                    Integer.parseInt(classVersion)
            );
            return resolvePortable(externalFields, classDefinition, isKey);
        }

        return null;
    }

    // TODO: extract to util class ???
    public static ClassDefinition lookupClassDefinition(
            InternalSerializationService serializationService,
            int factoryId,
            int classId,
            int classVersion
    ) {
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(factoryId, classId, classVersion);
        if (classDefinition == null) {
            throw QueryException.dataException(
                    format("Unable to find class definition for factoryId: %s, classId: %s, classVersion: %s",
                            factoryId, classId, classVersion)
            );
        }
        return classDefinition;
    }

    private static MapOptionsMetadata resolvePortable(
            List<ExternalField> externalFields,
            ClassDefinition classDefinition,
            boolean isKey
    ) {
        LinkedHashMap<String, QueryPath> fields = new LinkedHashMap<>();

        // TODO: validate type mismatches ???
        for (ExternalField externalField : externalFields) {
            String fieldName = externalField.name();

            if (classDefinition.hasField(fieldName)) {
                fields.put(fieldName, new QueryPath(fieldName, isKey));
            }
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PortableUpsertTargetDescriptor(
                        classDefinition.getFactoryId(),
                        classDefinition.getClassId(),
                        classDefinition.getVersion()
                ),
                new LinkedHashMap<>(fields)
        );
    }
}
