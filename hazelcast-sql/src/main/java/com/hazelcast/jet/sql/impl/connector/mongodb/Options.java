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

import com.hazelcast.jet.mongodb.datalink.MongoDataLink;
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

    static final String DATA_LINK_REF_OPTION = "data-link-name";
    static final String CONNECTION_STRING_OPTION = "connectionString";
    static final String DATABASE_NAME_OPTION = "database";
    static final String START_AT_OPTION = "startAt";
    static final String PK_COLUMN = "idColumn";
    private static final String POSSIBLE_VALUES = "This property should " +
            " have value of: a) 'now' b) time in epoch milliseconds or c) " +
            " ISO-formatted instant in UTC timezone, like '2023-03-24T15:31:00Z'.";

    private Options() {
    }

    static BsonTimestamp startAt(Map<String, String> options) {
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

    static String getDatabaseName(NodeEngine nodeEngine, Map<String, String> options) {
        String name = options.get(Options.DATABASE_NAME_OPTION);
        if (name != null) {
            return name;
        }
        if (options.containsKey(DATA_LINK_REF_OPTION)) {
            MongoDataLink link =
                    nodeEngine.getDataLinkService().getAndRetainDataLink(options.get(DATA_LINK_REF_OPTION), MongoDataLink.class);
            try {
                name = link.getDatabaseName();
                if (name != null) {
                    return name;
                }
            } finally {
                link.release();
            }
        }
        throw new IllegalArgumentException(DATABASE_NAME_OPTION + " must be provided in the mapping or data link.");
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
}
