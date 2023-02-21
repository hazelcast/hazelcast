/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapstore.validators;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.mapstore.GenericMapStoreProperties;
import com.hazelcast.sql.SqlRowMetadata;

import java.util.stream.Stream;

import static com.hazelcast.sql.SqlRowMetadata.COLUMN_NOT_FOUND;

public final class ExistingMappingValidator {

    private ExistingMappingValidator() {
    }

    public static void validateColumnsExist(SqlRowMetadata sqlRowMetadata, GenericMapStoreProperties properties) {
        // If GenericMapStoreProperties has columns defined, they must exist on the database
        Stream.concat(Stream.of(properties.idColumn), properties.columns.stream())
                .distinct() // avoid duplicate id column if present in columns property
                .forEach(columnName -> validateColumn(sqlRowMetadata, columnName));
    }

    public static int validateColumn(SqlRowMetadata sqlRowMetadata, String columnName) {
        int column = sqlRowMetadata.findColumn(columnName);
        if (column == COLUMN_NOT_FOUND) {
            throw new HazelcastException("Column '" + columnName + "' not found");
        }
        return column;
    }
}
