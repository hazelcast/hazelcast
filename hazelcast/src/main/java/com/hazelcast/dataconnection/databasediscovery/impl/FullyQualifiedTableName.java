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

package com.hazelcast.dataconnection.databasediscovery.impl;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.Stream;

class FullyQualifiedTableName {

    private final String catalogName;

    private final String schemaName;

    private final String tableName;

    FullyQualifiedTableName(String catalogName, String schemaName, String tableName) {
        this.catalogName = Objects.toString(catalogName, "");
        this.schemaName = Objects.toString(schemaName, "");
        this.tableName = Objects.toString(tableName, "");
    }

    @Nonnull
    public String getCatalogName() {
        return catalogName;
    }

    @Nonnull
    public String getSchemaName() {
        return schemaName;
    }

    @Nonnull
    public String getTableName() {
        return tableName;
    }

    public String[] getName() {
        return Stream.of(catalogName, schemaName, tableName)
                .filter(s -> !s.isBlank())
                .toArray(String[]::new);
    }
}
