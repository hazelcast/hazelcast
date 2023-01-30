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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class QueriesTest {

    final String mapping = "mymapping";

    final String idColumn = "id";

    final List<SqlColumnMetadata> columnMetadata = Arrays.asList(
            new SqlColumnMetadata("id", SqlColumnType.INTEGER, false),
            new SqlColumnMetadata("name", SqlColumnType.VARCHAR, true),
            new SqlColumnMetadata("address", SqlColumnType.VARCHAR, true)
    );

    @Test
    public void testLoad() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.load();
        Assert.assertEquals("SELECT * FROM \"mymapping\" WHERE \"id\" = ?", result);
    }

    @Test
    public void testLoadAll() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.loadAll(2);
        Assert.assertEquals("SELECT * FROM \"mymapping\" WHERE \"id\" IN (?, ?)", result);
    }

    @Test
    public void testLoadAllKeys() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.loadAllKeys();
        Assert.assertEquals("SELECT \"id\" FROM \"mymapping\"", result);
    }

    @Test
    public void testStoreInsert() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.storeInsert();
        Assert.assertEquals("SINK INTO \"mymapping\" (\"id\", \"name\", \"address\") VALUES (?, ?, ?)", result);
    }

    @Test
    public void testStoreUpdate() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.storeUpdate();
        Assert.assertEquals("UPDATE \"mymapping\" SET \"name\" = ?, \"address\" = ? WHERE \"id\" = ?", result);
    }

    @Test
    public void testDelete() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.delete();
        Assert.assertEquals("DELETE FROM \"mymapping\" WHERE \"id\" = ?", result);
    }

    @Test
    public void testDeleteAll() {
        Queries queries = new Queries(mapping, idColumn, columnMetadata);
        String result = queries.deleteAll(2);
        Assert.assertEquals("DELETE FROM \"mymapping\" WHERE \"id\" IN (?, ?)", result);
    }
}
