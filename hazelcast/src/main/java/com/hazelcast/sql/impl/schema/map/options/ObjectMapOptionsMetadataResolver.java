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
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.ObjectUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

// TODO: deduplicate with MapSampleMetadataResolver
public class ObjectMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String fieldName = isKey ? KEY_ATTRIBUTE_NAME.value() : THIS_ATTRIBUTE_NAME.value();
        int fieldIndex = externalFields.stream().map(ExternalField::name).collect(toList()).indexOf(fieldName);
        if (fieldIndex > -1) {
            QueryDataType type = externalFields.get(fieldIndex).type();
            QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
            return new MapOptionsMetadata(
                    GenericQueryTargetDescriptor.INSTANCE,
                    ObjectUpsertTargetDescriptor.INSTANCE,
                    new LinkedHashMap<>(singletonMap(fieldName, new MapTableField(fieldName, type, false, path)))
            );
        }
        return null;
    }
}
