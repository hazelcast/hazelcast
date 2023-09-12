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

class FullyQualifiedTableName {

    private String catalogName = "";

    private String schemaName = "";

    private String tableName = "";

    FullyQualifiedTableName(String[] name) {

        if (name.length == 3) {
            catalogName = name[0];
            schemaName = name[1];
            tableName = name[2];
        } else if (name.length == 2) {
            schemaName = name[0];
            tableName = name[1];
        } else if (name.length == 1) {
            tableName = name[0];
        }
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }
}
