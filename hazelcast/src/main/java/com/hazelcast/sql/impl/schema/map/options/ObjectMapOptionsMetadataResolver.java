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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;
import static java.util.Collections.singletonMap;

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
        int fieldIndex = indexOf(externalFields, fieldName);

        if (fieldIndex > -1) {
            // TODO: validate type mismatch ???
            QueryPath path = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
            return new MapOptionsMetadata(
                    GenericQueryTargetDescriptor.INSTANCE,
                    ObjectUpsertTargetDescriptor.INSTANCE,
                    new LinkedHashMap<>(singletonMap(fieldName, path))
            );
        }

        return null;
    }

    private static int indexOf(List<ExternalField> externalFields, String fieldName) {
        for (int i = 0; i < externalFields.size(); i++) {
            if (externalFields.get(i).name().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }
}
