/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.contrib.mongodb;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.function.FunctionEx.identity;

/**
 * Contains factory methods for MongoDB sources.
 * <p>
 * See {@link MongoDBSourceBuilder} for creating custom MongoDB sources.
 */
public final class MongoDBSources {

    private MongoDBSources() {
    }

    /**
     * Returns a MongoDB batch source which queries the collection using given
     * {@code filter} and applies the given {@code projection} on the documents.
     * <p>
     * See {@link MongoDBSourceBuilder} for creating custom MongoDB sources.
     * <p>
     * Here's an example which queries documents in a collection having the
     * field {@code age} with a value greater than {@code 10} and applies a
     * projection so that only the {@code age} field is returned in the
     * emitted document.
     *
     * <pre>{@code
     * BatchSource<Document> batchSource =
     *         MongoDBSources.batch(
     *                 "batch-source",
     *                 "mongodb://127.0.0.1:27017",
     *                 "myDatabase",
     *                 "myCollection",
     *                 new Document("age", new Document("$gt", 10)),
     *                 new Document("age", 1)
     *         );
     * Pipeline p = Pipeline.create();
     * BatchStage<Document> srcStage = p.readFrom(batchSource);
     * }</pre>
     *
     * @param name             a descriptive name for the source (diagnostic purposes)
     * @param connectionString a connection string URI to MongoDB for example:
     *                         {@code mongodb://127.0.0.1:27017}
     * @param database         the name of the database
     * @param collection       the name of the collection
     * @param filter           filter object as a {@link Document}
     * @param projection       projection object as a {@link Document}
     */
    @Nonnull
    public static BatchSource<Document> batch(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return MongoDBSourceBuilder
                .batch(name, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(database))
                .collectionFn(db -> db.getCollection(collection))
                .destroyFn(MongoClient::close)
                .searchFn(col -> col.find().filter(filter).projection(projection))
                .mapFn(identity())
                .build();
    }

    /**
     * Returns a MongoDB stream source which watches the changes on the
     * collection. The source applies the given {@code filter} and {@code
     * projection} on the change stream documents.
     * <p>
     * Change stream is available for replica sets and sharded clusters that
     * use WiredTiger storage engine and replica set protocol version 1 (pv1).
     * Change streams can also be used on deployments which employ MongoDB's
     * encryption-at-rest feature. You cannot watch on system collections and
     * collections in admin, local and config databases.
     * <p>
     * See {@link MongoDBSourceBuilder} for creating custom MongoDB sources.
     * <p>
     * Here's an example which streams inserts on a collection having the
     * field {@code age} with a value greater than {@code 10} and applies a
     * projection so that only the {@code age} field is returned in the
     * emitted document.
     *
     * <pre>{@code
     * StreamSource<? extends Document> streamSource =
     *         MongoDBSources.stream(
     *                 "stream-source",
     *                 "mongodb://127.0.0.1:27017",
     *                 "myDatabase",
     *                 "myCollection",
     *                 new Document("fullDocument.age", new Document("$gt", 10))
     *                         .append("operationType", "insert"),
     *                 new Document("fullDocument.age", 1)
     *         );
     *
     * Pipeline p = Pipeline.create();
     * StreamSourceStage<? extends Document> srcStage = p.readFrom(streamSource);
     * }</pre>
     *
     * @param name             a descriptive name for the source (diagnostic purposes)
     * @param connectionString a connection string URI to MongoDB for example:
     *                         {@code mongodb://127.0.0.1:27017}
     * @param database         the name of the database
     * @param collection       the name of the collection
     * @param filter           filter object as a {@link Document}
     * @param projection       projection object as a {@link Document}
     */
    @Nonnull
    public static StreamSource<? extends Document> stream(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        return MongoDBSourceBuilder
                .stream(name, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(database))
                .collectionFn(db -> db.getCollection(collection))
                .destroyFn(MongoClient::close)
                .searchFn(
                        col -> {
                            List<Bson> aggregates = new ArrayList<>();
                            if (filter != null) {
                                aggregates.add(Aggregates.match(filter));
                            }
                            if (projection != null) {
                                aggregates.add(Aggregates.project(projection));
                            }
                            ChangeStreamIterable<? extends Document> watch;
                            if (aggregates.isEmpty()) {
                                watch = col.watch();
                            } else {
                                watch = col.watch(aggregates);
                            }
                            return watch;
                        }
                )
                .mapFn(ChangeStreamDocument::getFullDocument)
                .build();
    }
}
