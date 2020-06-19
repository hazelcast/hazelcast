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

package com.hazelcast.sql;

/**
 * SQL column metadata.
 */
public class SqlColumnMetadata {

    private final String name;
    private final SqlColumnType type;

    public SqlColumnMetadata(String name, SqlColumnType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Get column name.
     *
     * @return Column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets column type.
     *
     * @return Column type.
     */
    public SqlColumnType getType() {
        return type;
    }
}
