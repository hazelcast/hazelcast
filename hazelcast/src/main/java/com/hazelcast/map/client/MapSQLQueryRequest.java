/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.client;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.util.IterationType;
import java.io.IOException;

public final class MapSQLQueryRequest extends AbstractMapQueryRequest {

    private String sql;

    public MapSQLQueryRequest() {
    }

    public MapSQLQueryRequest(String name, String sql, IterationType iterationType) {
        super(name, iterationType);
        this.sql = sql;
    }

    @Override
    protected Predicate getPredicate() {
        return new SqlPredicate(sql);
    }

    public int getClassId() {
        return MapPortableHook.SQL_QUERY;
    }

    protected void writePortableInner(PortableWriter writer) throws IOException {
        writer.writeUTF("sql", sql);
    }

    protected void readPortableInner(PortableReader reader) throws IOException {
        sql = reader.readUTF("sql");
    }
}
