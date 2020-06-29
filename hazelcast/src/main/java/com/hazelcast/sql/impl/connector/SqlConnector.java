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

package com.hazelcast.sql.impl.connector;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

// TODO: make this class public for user connectors
//   (TableSchemaField, Table, TableField, QueryDataType etc. need to be public then?)
public interface SqlConnector {

    String JAVA_SERIALIZATION_FORMAT = "java";
    String PORTABLE_SERIALIZATION_FORMAT = "portable";
    String JSON_SERIALIZATION_FORMAT = "json";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the accessed object name. If missing, the external table name
     * itself is used.
     */
    String TO_OBJECT_NAME = "objectName";

    /**
     * Return the name of the connector as seen in the {@code TYPE} clause in
     * the {@code CREATE EXTERNAL TABLE} command.
     */
    String typeName();

    /**
     * Creates a Table object with the given fields. Will not attempt to
     * connect to the remote service.
     *
     * @param externalFields optional list of fields. If {@code null},
     *     an attempt to resolve them automatically will be made. If not
     *     successful, an exception will be thrown. An empty list is
     *     valid, it means there are zero columns in the table: you can
     *     still query hidden fields or count the records. No validation
     *     is performed: if fields are given, no connection to remote
     *     source is made.
     */
    @Nonnull
    Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> externalFields
    );
}
