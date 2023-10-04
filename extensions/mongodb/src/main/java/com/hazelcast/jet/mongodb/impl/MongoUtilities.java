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
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.jet.JetException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.ClusterDescription;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.unset;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class MongoUtilities {

    /**
     * A predicate that matches all rows in update queries.
     */
    public static final Document UPDATE_ALL_PREDICATE = Document.parse("{}");

    private MongoUtilities() {
    }

    /**
     * Adds an aggregate to the query, that for each document will calculate
     * {@code document._id.getTimestamp().getTime() MOD totalParallelism == processorIndex}, using MongoDB
     * <a href="https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/">function aggregate</a>.
     * <p>
     * Only if this comparison is true, the document will be read by given processor (filtering is done on MongoDB
     * side as well).
     * <p>
     * Additional, temporary column {@code _thisPartition} will be created during this calculation and will be removed
     * at the end of calculation.
     */
    @Nonnull
    static List<Bson> partitionAggregate(int totalParallelism, int processorIndex, boolean stream) {
        List<Bson> aggregateList = new ArrayList<>(3);

        String hashFunction = "function hash(s) { \n" +
                "  var hash = 0, i, chr;\n" +
                "  if (s.length === 0) return hash;\n" +
                "  for (i = 0; i < s.length; i++) {\n" +
                "    chr = s.charCodeAt(i);\n" +
                "    hash = ((hash << 5) - hash) + chr;\n" +
                "    hash |= 0;\n" +
                "  }\n" +
                "  return hash;\n" +
                "}\n";

        String moduloPart = " %" + totalParallelism + " == " + processorIndex + ";\n";
        String code = "function(id) {\n"
                + "if (id instanceof ObjectId)"
                + "    return id === null ? -1 : id.getTimestamp().getTime() " + moduloPart
                + "if (typeof(id) === 'number' || typeof(id) === 'bigint') return id " + moduloPart
                + "if (typeof(id) === 'string') {\n"
                + hashFunction
                + "    return hash(id) " + moduloPart
                + "}\n"
                + "else return 0;\n"
                + "}";

        String idRef = stream ? "$fullDocument._id" : "$_id";
        Document functionInv = new Document("$function",
                new Document("lang", "js")
                        .append("body", code)
                        .append("args", new BsonArray(singletonList(new BsonString(idRef)))));

        aggregateList.add(addFields(new Field<>("_thisPartition", functionInv)));
        aggregateList.add(match(Filters.eq("_thisPartition", true)));
        aggregateList.add(unset("_thisPartition"));
        return aggregateList;
    }

    /**
     * Converts given time in millisecond of unix epoch to BsonTimestamp.
     */
    public static BsonTimestamp bsonTimestampFromTimeMillis(long time) {
        return new BsonTimestamp((int) MILLISECONDS.toSeconds(time), 0);
    }

    /**
     * Converts given bson timest1amp to unix epoch.
     */
    @Nullable
    public static BsonTimestamp localDateTimeToTimestamp(@Nullable LocalDateTime time) {
        if (time == null) {
            return null;
        }

        return new BsonTimestamp((int) time.atZone(systemDefault()).withZoneSameInstant(UTC).toEpochSecond(), 0);
    }

    /**
     * Converts given bson timestamp to unix epoch.
     */
    @Nullable
    public static LocalDateTime bsonDateTimeToLocalDateTime(@Nullable BsonDateTime time) {
        if (time == null) {
            return null;
        }
        Instant instant = Instant.ofEpochMilli(time.getValue());
        return instant.atZone(UTC).withZoneSameInstant(systemDefault()).toLocalDateTime();
    }

    /**
     * Converts given bson timestamp to unix epoch.
     */
    @Nullable
    public static LocalDateTime bsonTimestampToLocalDateTime(@Nullable BsonTimestamp time) {
        if (time == null) {
            return null;
        }
        long v = time.getTime();
        return LocalDateTime.ofEpochSecond(v, 0, UTC)
                            .atZone(UTC)
                            .withZoneSameInstant(systemDefault())
                            .toLocalDateTime();
    }


    public static void checkDatabaseAndCollectionExists(MongoClient client, String databaseName, String collectionName) {
        checkDatabaseExists(client, databaseName);
        MongoDatabase database = client.getDatabase(databaseName);
        if (collectionName != null) {
            checkCollectionExists(database, collectionName);
        }
    }

    static void checkCollectionExists(MongoDatabase database, String collectionName) {
        for (String name : database.listCollectionNames()) {
            if (name.equals(collectionName)) {
                return;
            }
        }
        throw new JetException("Collection " + collectionName + " in database " + database.getName() + " does not exist");
    }

    static void checkDatabaseExists(MongoClient client, String databaseName) {
        for (String name : client.listDatabaseNames()) {
            if (name.equalsIgnoreCase(databaseName)) {
                return;
            }
        }
        ClusterDescription clusterDescription = client.getClusterDescription();
        throw new JetException("Database " + databaseName + " does not exist in cluster " + clusterDescription);
    }

}
