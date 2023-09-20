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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import javax.annotation.Nonnull;

final class FromSqlRowConverter {

    private FromSqlRowConverter() {
    }

    // Convert SqlRow to GenericRecord
    @Nonnull
    public static GenericRecord toGenericRecord(SqlRow sqlRow, GenericMapStoreProperties properties) {
        GenericRecordBuilder builder = GenericRecordBuilder.compact(properties.compactTypeName);

        SqlRowMetadata sqlRowMetadata = sqlRow.getMetadata();
        for (int i = 0; i < sqlRowMetadata.getColumnCount(); i++) {
            SqlColumnMetadata sqlColumnMetadata = sqlRowMetadata.getColumn(i);

            String columnName = sqlColumnMetadata.getName();
            if (columnName.equals(properties.idColumn) && !properties.idColumnInColumns) {
                continue;
            }

            switch (sqlColumnMetadata.getType()) {
                case VARCHAR:
                    builder.setString(columnName, sqlRow.getObject(i));
                    break;

                case BOOLEAN:
                    builder.setBoolean(columnName, sqlRow.getObject(i));
                    break;

                case TINYINT:
                    builder.setInt8(columnName, sqlRow.getObject(i));
                    break;

                case SMALLINT:
                    builder.setInt16(columnName, sqlRow.getObject(i));
                    break;

                case INTEGER:
                    builder.setInt32(columnName, sqlRow.getObject(i));
                    break;

                case BIGINT:
                    builder.setInt64(columnName, sqlRow.getObject(i));
                    break;

                case DECIMAL:
                    builder.setDecimal(columnName, sqlRow.getObject(i));
                    break;

                case REAL:
                    builder.setFloat32(columnName, sqlRow.getObject(i));
                    break;

                case DOUBLE:
                    builder.setFloat64(columnName, sqlRow.getObject(i));
                    break;

                case DATE:
                    builder.setDate(columnName, sqlRow.getObject(i));
                    break;

                case TIME:
                    builder.setTime(columnName, sqlRow.getObject(i));
                    break;

                case TIMESTAMP:
                    builder.setTimestamp(columnName, sqlRow.getObject(i));
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    builder.setTimestampWithTimezone(columnName, sqlRow.getObject(i));
                    break;

                default:
                    throw new HazelcastException("Column type " + sqlColumnMetadata.getType() + " not supported");
            }
        }

        return builder.build();
    }
}
