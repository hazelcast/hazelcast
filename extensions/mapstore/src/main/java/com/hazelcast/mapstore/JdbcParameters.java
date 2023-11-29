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
import com.hazelcast.sql.SqlColumnMetadata;

import java.util.List;

class JdbcParameters {
    private int idPos;

    private Object[] params;

    void setIdPos(int idPos) {
        this.idPos = idPos;
    }

    Object[] getParams() {
        return params;
    }

    void setParams(Object[] params) {
        this.params = params;
    }

    void shiftIdParameterToEnd() {
        Object id = params[idPos];
        for (int i = idPos; i < params.length - 1; i++) {
            params[i] = params[i + 1];
        }
        params[params.length - 1] = id;
    }

    // Convert key and GenericRecord to JDBC parameter values
    static <K, V> JdbcParameters convert(
            K key,
            V value,
            List<SqlColumnMetadata> columnMetadataList,
            String idColumn,
            boolean singleColumnAsValue
    ) {

        JdbcParameters jdbcParameters = new JdbcParameters();

        int idPos = -1;
        Object[] params = new Object[columnMetadataList.size()];

        // Iterate over columns
        for (int i = 0; i < columnMetadataList.size(); i++) {
            SqlColumnMetadata columnMetadata = columnMetadataList.get(i);

            // Get column name
            String columnName = columnMetadata.getName();

            // If column name is primary key, use the key value
            if (columnName.equals(idColumn)) {
                idPos = i;
                params[i] = key;
                continue;
            }
            if (columnMetadataList.size() == 2 && singleColumnAsValue) {
                // If we only have a single column as value, we get it as it is.
                params[i] = value;
            } else {
                // Get all other values from GenericRecord
                GenericRecord genericRecord = (GenericRecord) value;
                switch (columnMetadata.getType()) {
                    case VARCHAR:
                        params[i] = genericRecord.getString(columnName);
                        break;

                    case BOOLEAN:
                        params[i] = genericRecord.getBoolean(columnName);
                        break;

                    case TINYINT:
                        params[i] = genericRecord.getInt8(columnName);
                        break;

                    case SMALLINT:
                        params[i] = genericRecord.getInt16(columnName);
                        break;

                    case INTEGER:
                        params[i] = genericRecord.getInt32(columnName);
                        break;

                    case BIGINT:
                        params[i] = genericRecord.getInt64(columnName);
                        break;

                    case REAL:
                        params[i] = genericRecord.getFloat32(columnName);
                        break;

                    case DOUBLE:
                        params[i] = genericRecord.getFloat64(columnName);
                        break;

                    case DATE:
                        params[i] = genericRecord.getDate(columnName);
                        break;

                    case TIME:
                        params[i] = genericRecord.getTime(columnName);
                        break;

                    case TIMESTAMP:
                        params[i] = genericRecord.getTimestamp(columnName);
                        break;

                    case TIMESTAMP_WITH_TIME_ZONE:
                        params[i] = genericRecord.getTimestampWithTimezone(columnName);
                        break;

                    case DECIMAL:
                        params[i] = genericRecord.getDecimal(columnName);
                        break;

                    default:
                        throw new HazelcastException("Column type " + columnMetadata.getType() + " not supported");
                    }
                }
            }


        jdbcParameters.setParams(params);
        jdbcParameters.setIdPos(idPos);

        return jdbcParameters;
    }

}
