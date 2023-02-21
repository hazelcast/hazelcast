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

        SqlRowMetadata metadata = sqlRow.getMetadata();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            SqlColumnMetadata column = metadata.getColumn(i);

            if (column.getName().equals(properties.idColumn) && !properties.idColumnInColumns) {
                continue;
            }

            switch (column.getType()) {
                case VARCHAR:
                    builder.setString(column.getName(), sqlRow.getObject(i));
                    break;

                case BOOLEAN:
                    builder.setBoolean(column.getName(), sqlRow.getObject(i));
                    break;

                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    builder.setInt32(column.getName(), sqlRow.getObject(i));
                    break;

                case BIGINT:
                    builder.setInt64(column.getName(), sqlRow.getObject(i));
                    break;

                case DECIMAL:
                    builder.setDecimal(column.getName(), sqlRow.getObject(i));
                    break;

                case REAL:
                    builder.setFloat32(column.getName(), sqlRow.getObject(i));
                    break;

                case DOUBLE:
                    builder.setFloat64(column.getName(), sqlRow.getObject(i));
                    break;

                case DATE:
                    builder.setDate(column.getName(), sqlRow.getObject(i));
                    break;

                case TIME:
                    builder.setTime(column.getName(), sqlRow.getObject(i));
                    break;

                case TIMESTAMP:
                    builder.setTimestamp(column.getName(), sqlRow.getObject(i));
                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    builder.setTimestampWithTimezone(column.getName(), sqlRow.getObject(i));
                    break;

                default:
                    throw new HazelcastException("Column type " + column.getType() + " not supported");
            }
        }

        return builder.build();
    }
}
