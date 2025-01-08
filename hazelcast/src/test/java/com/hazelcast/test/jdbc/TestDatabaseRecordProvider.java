/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.test.jdbc;

import java.util.Arrays;
import java.util.List;

/**
 * Allows creating of objects (tables, collections) used in GenericMapStore tests.
 */
public interface TestDatabaseRecordProvider {

    /**
     * Creates a database object (table, collection, etc) with two fields:
     * - id or {@code person-id}, depending on {@code #useQuotedNames} option
     * - name
     */
    ObjectSpec createObject(String objectName, boolean useQuotedNames);

    /**
     * Creates a database object (table, collection, etc) with two fields:
     * - id or {@code person-id}, depending on {@code #useQuotedNames} option
     * - name
     */
    default ObjectSpec createObject(String objectName) {
        return createObject(objectName, false);
    }

    /**
     * Creates a database object (table, collection, etc) with provided fields.
     */
    void createObject(ObjectSpec spec);

    /**
     * Inserts {@code count} items to object {@code spec.name}
     */
    void insertItems(ObjectSpec spec, int count);

    /**
     * Returns underlying provider
     */
    TestDatabaseProvider provider();

    /**
     * Checks if objects in database matches listed rows.
     */
    void assertRows(String objectName, List<List<Object>> rows);

    /**
     * Checks if objects in database matches listed rows.
     */
    default void assertRow(String objectName, List<Object> row) {
        assertRows(objectName, List.of(row));
    }

    default void assertRows(String objectName, List<Class<?>> columnType, List<List<Object>> rows) {
        assertRows(objectName, rows);
    }

    default void assertRow(String objectName, List<Class<?>> columnType, List<Object> row) {
        assertRows(objectName, columnType, List.of(row));
    }

    class ObjectSpec {
        public final String name;
        public final List<Column> columns;

        public ObjectSpec(String name, Column... columns) {
            this.name = name;
            this.columns = Arrays.asList(columns);
        }
    }

    enum ColumnType {
        STRING, INT
    }

    class Column {
        public final String name;
        public final ColumnType type;
        public final boolean primaryKey;

        public Column(String name, ColumnType type, boolean primaryKey) {
            this.name = name;
            this.type = type;
            this.primaryKey = primaryKey;
        }

        public static Column col(String name, ColumnType type) {
            return new Column(name, type, false);
        }
        public static Column col(String name, ColumnType type, boolean primaryKey) {
            return new Column(name, type, primaryKey);
        }
    }
}
