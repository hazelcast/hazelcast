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

package com.hazelcast.sql.impl.parser;

import com.hazelcast.sql.impl.schema.Catalog;
import com.hazelcast.sql.impl.schema.TableSchema;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Map.Entry;

/**
 * 'CREATE TABLE' DDL statement.
 */
public class CreateTableStatement implements DdlStatement {

    private final TableSchema schema;

    private final boolean replace;
    private final boolean ifNotExists;

    public CreateTableStatement(String name,
                                String type,
                                List<Entry<String, QueryDataType>> fields,
                                List<Entry<String, String>> options,
                                boolean replace,
                                boolean ifNotExists) {
        this.schema = new TableSchema(name, type, fields, options);

        this.replace = replace;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void execute(Catalog catalog) {
        catalog.createTable(schema, replace, ifNotExists);
    }
}
