/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoader;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

class GenericMapLoader<K> implements MapLoader<K, GenericRecord> {

    protected SqlService sql;

    protected Queries queries;

    protected GenericMapStoreProperties properties;

    @Nonnull
    private GenericRecord convertRowToGenericRecord(SqlRow row) {
        GenericRecordBuilder builder = GenericRecordBuilder.compact(properties.compactTypeName);

        SqlRowMetadata metadata = row.getMetadata();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            SqlColumnMetadata column = metadata.getColumn(i);

            if (column.getName().equals(properties.idColumn) && !properties.idColumnInColumns) {
                continue;
            }

            switch (column.getType()) {
                case VARCHAR:
                    builder.setString(column.getName(), row.getObject(i));
                    break;

                case BOOLEAN:
                    builder.setBoolean(column.getName(), row.getObject(i));
                    break;

                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    builder.setInt32(column.getName(), row.getObject(i));
                    break;

                case BIGINT:
                    builder.setInt64(column.getName(), row.getObject(i));
                    break;

                case DECIMAL:
                    builder.setDecimal(column.getName(), row.getObject(i));
                    break;

                case REAL:
                    builder.setFloat32(column.getName(), row.getObject(i));
                    break;

                case DOUBLE:
                    builder.setFloat64(column.getName(), row.getObject(i));
                    break;

                case DATE:
                    builder.setDate(column.getName(), row.getObject(i));
                    break;

                case TIME:
                    builder.setTime(column.getName(), row.getObject(i));
                    break;

                case TIMESTAMP:
                    builder.setTimestamp(column.getName(), row.getObject(i));
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    builder.setTimestampWithTimezone(column.getName(), row.getObject(i));
                    break;

                default:
                    throw new HazelcastException("Column type " + column.getType() + " not supported");
            }
        }

        return builder.build();
    }

    protected void initMapLoader(HazelcastInstance instance, Properties properties, String mapName) {
        this.properties = new GenericMapStoreProperties(properties, mapName);
        sql = instance.getSql();
    }
    @Override
    public GenericRecord load(K key) {
        try (SqlResult queryResult = sql.execute(queries.load(), key)) {
            Iterator<SqlRow> it = queryResult.iterator();
            if (it.hasNext()) {
                SqlRow row = it.next();
                if (it.hasNext()) {
                    throw new IllegalStateException("multiple matching rows for a key " + key);
                }
                return convertRowToGenericRecord(row);
            } else {
                return null;
            }
        }
    }

    @Override
    public Map<K, GenericRecord> loadAll(Collection<K> keys) {
        Object[] keysArray = keys.toArray();

        try (SqlResult queryResult = sql.execute(queries.loadAll(keys.size()), keysArray)) {
            Iterator<SqlRow> it = queryResult.iterator();

            Map<K, GenericRecord> result = new HashMap<>();
            while (it.hasNext()) {
                SqlRow row = it.next();
                K id = row.getObject(properties.idColumn);
                GenericRecord genericRecord = convertRowToGenericRecord(row);
                result.put(id, genericRecord);
            }
            return result;
        }

    }

    @Override
    public Iterable<K> loadAllKeys() {
        SqlResult keysResult = sql.execute(queries.loadAllKeys());

        // The contract for loadAllKeys says that if iterator implements Closable
        // then it will be closed when the iteration is over
        return () -> new MappingClosingIterator<>(
                keysResult.iterator(),
                (SqlRow row) -> row.getObject(properties.idColumn),
                keysResult::close
        );
    }

}
