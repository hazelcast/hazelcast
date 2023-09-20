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

package com.hazelcast.mapstore;

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueriesTest {

    final String mapping = "mymapping";
    final String mappingEscape = "my\"mapping";

    final String idColumn = "id";
    final String idColumnEscape = "i\"d";

    final List<SqlColumnMetadata> columnMetadata = Arrays.asList(
            new SqlColumnMetadata("id", SqlColumnType.INTEGER, false),
            new SqlColumnMetadata("name", SqlColumnType.VARCHAR, true),
            new SqlColumnMetadata("address", SqlColumnType.VARCHAR, true)
    );

    final List<SqlColumnMetadata> columnMetadataEscape = Arrays.asList(
            new SqlColumnMetadata("i\"d", SqlColumnType.INTEGER, false),
            new SqlColumnMetadata("na\"me", SqlColumnType.VARCHAR, true),
            new SqlColumnMetadata("add\"ress", SqlColumnType.VARCHAR, true)
    );

    @Test
    public void testLoadIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.load();
        assertEquals("SELECT * FROM \"mymapping\" WHERE \"id\" = ?", result);
    }

    @Test
    public void testLoadIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.load();
        assertEquals("SELECT * FROM \"my\"\"mapping\" WHERE \"i\"\"d\" = ?", result);
    }

    @Test
    public void testLoadAllIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.loadAll(2);
        assertEquals("SELECT * FROM \"mymapping\" WHERE \"id\" IN (?, ?)", result);
    }

    @Test
    public void testLoadAllIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.loadAll(2);
        assertEquals("SELECT * FROM \"my\"\"mapping\" WHERE \"i\"\"d\" IN (?, ?)", result);
    }

    @Test
    public void testLoadAllKeysIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.loadAllKeys();
        assertEquals("SELECT \"id\" FROM \"mymapping\"", result);
    }

    @Test
    public void testLoadAllKeysIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.loadAllKeys();
        assertEquals("SELECT \"i\"\"d\" FROM \"my\"\"mapping\"", result);
    }

    @Test
    public void testStoreSinkIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.storeSink();
        assertEquals("SINK INTO \"mymapping\" (\"id\", \"name\", \"address\") VALUES (?, ?, ?)", result);
    }

    @Test
    public void testStoreSinkIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.storeSink();
        assertEquals("SINK INTO \"my\"\"mapping\" (\"i\"\"d\", \"na\"\"me\", \"add\"\"ress\") VALUES (?, ?, ?)", result);
    }

    @Test
    public void testStoreUpdateIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.storeUpdate();
        assertEquals("UPDATE \"mymapping\" SET \"name\" = ?, \"address\" = ? WHERE \"id\" = ?", result);
    }

    @Test
    public void testStoreUpdateIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.storeUpdate();
        assertEquals("UPDATE \"my\"\"mapping\" SET \"na\"\"me\" = ?, \"add\"\"ress\" = ? WHERE \"i\"\"d\" = ?", result);
    }

    @Test
    public void testDeleteIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.delete();
        assertEquals("DELETE FROM \"mymapping\" WHERE \"id\" = ?", result);
    }

    @Test
    public void testDeleteIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.delete();
        assertEquals("DELETE FROM \"my\"\"mapping\" WHERE \"i\"\"d\" = ?", result);
    }

    @Test
    public void testDeleteAllIsQuoted() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.deleteAll(2);
        assertEquals("DELETE FROM \"mymapping\" WHERE \"id\" IN (?, ?)", result);
    }

    @Test
    public void testDeleteAllIsEscaped() {
        Queries queries = new Queries(mappingEscape, idColumnEscape, columnMetadataEscape);
        String result = queries.deleteAll(2);
        assertEquals("DELETE FROM \"my\"\"mapping\" WHERE \"i\"\"d\" IN (?, ?)", result);
    }
}
