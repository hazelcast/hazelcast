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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.jet.mongodb.ResourceChecks;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import com.hazelcast.jet.mongodb.impl.MongoUtilities;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;
import org.bson.BsonTimestamp;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

final class Options {

    /**
     * A valid MongoDB connectionString.
     * Must be non-empty (if set).
     * <p>Not mandatory if data connection is provided in mapping definition.</p>
     */
    static final String CONNECTION_STRING_OPTION = "connectionString";

    /**
     * Option for streaming source only and mandatory for them.
     * <p>
     * Indicates a moment from which the oplog (changeStream) will be read. Possible values:
     * <ul>
     *     <li>string 'now' - sets current time (of submitting the mapping) as the start time</li>
     *     <li>numeric value that means milliseconds of unix epoch</li>
     *     <li>ISO-formatted instant in UTC timezone, like '2023-03-24T15:31:00Z'</li>
     * </ul>
     */
    static final String START_AT_OPTION = "startAt";

    /**
     * The name of the column that will be used as primary key.
     * Note it's the name in Hazelcast, not external name.
     * <p>Setting this option allows user to avoid using {@code _id} column as primary key, e.g. if user
     * prefers to use some natural key instead of artificial key they added at project start.
     *
     * <p>Setting this property is not mandatory, by default connector will pick column that maps to Mongo's
     * {@code _id} column.</p>
     */
    static final String PK_COLUMN = "idColumn";

    /**
     * If set to true, the reading and/or writing from/to MongoDB will be done in one processor instance.
     * <p>
     * Normally user wants to distribute the work, however the {@code $function} aggregate is not present on
     * e.g. Atlas Serverless instances. In such cases setting this property to {@code true} allows user
     * to query the Atlas Serverless - in one processor only, but better one than nothing. Maybe some day MongoDB will
     * change that restriction.
     */
    static final String FORCE_READ_PARALLELISM_ONE = "forceReadTotalParallelismOne";

    /**
     * If set to true, the reading will be preceded with checking the existence of database and collection.
     */
    static final String CHECK_EXISTENCE = "checkExistence";

    private static final String POSSIBLE_VALUES = "This property should " +
            " have value of: a) 'now' b) time in epoch milliseconds or c) " +
            " ISO-formatted instant in UTC timezone, like '2023-03-24T15:31:00Z'.";

    private Options() {
    }

    static BsonTimestamp startAtTimestamp(Map<String, String> options) {
        String startAtValue = options.get(START_AT_OPTION);
        if (isNullOrEmpty(startAtValue)) {
            throw QueryException.error("startAt property is required for MongoDB stream. " + POSSIBLE_VALUES);
        }
        if ("now".equalsIgnoreCase(startAtValue)) {
            return MongoUtilities.bsonTimestampFromTimeMillis(System.currentTimeMillis());
        } else {
            try {
                return MongoUtilities.bsonTimestampFromTimeMillis(Long.parseLong(startAtValue));
            } catch (NumberFormatException e) {
                try {
                    return MongoUtilities.bsonTimestampFromTimeMillis(Instant.parse(startAtValue).toEpochMilli());
                } catch (DateTimeParseException ex) {
                    throw QueryException.error("Invalid startAt value: '" + startAtValue + "'. " + POSSIBLE_VALUES);
                }
            }
        }
    }

    static String getDatabaseName(NodeEngine nodeEngine, String[] externalName, String dataConnectionName) {
        if (externalName.length == 2) {
            return externalName[0];
        }
        if (dataConnectionName != null) {
            MongoDataConnection dataConnection =
                    nodeEngine.getDataConnectionService().getAndRetainDataConnection(
                            dataConnectionName, MongoDataConnection.class);
            try {
                String name = dataConnection.getDatabaseName();
                if (name != null) {
                    return name;
                }
            } finally {
                dataConnection.release();
            }
        }
        throw new IllegalArgumentException("Database must be provided in the mapping or data connection.");
    }

    static Predicate<MappingField> getPkColumnChecker(Map<String, String> options, boolean isStreaming) {
        boolean nonDefault = options.containsKey(PK_COLUMN);

        String defaultIdName = isStreaming ? "fullDocument._id" : "_id";
        String value = options.getOrDefault(PK_COLUMN, defaultIdName);
        if (nonDefault) {
            return mf -> mf.name().equalsIgnoreCase(value);
        } else {
            return mf -> mf.externalName().equalsIgnoreCase(value);
        }
    }

    static ResourceChecks readExistenceChecksFlag(Map<String, String> options) {
        return ResourceChecks.fromString(options.getOrDefault(CHECK_EXISTENCE, "only-initial"));
    }
}
