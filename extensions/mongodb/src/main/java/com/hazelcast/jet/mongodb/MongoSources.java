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

package com.hazelcast.jet.mongodb;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.mongodb.MongoSourceBuilder.Batch;
import com.hazelcast.jet.mongodb.MongoSourceBuilder.Stream;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.StreamSource;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonTimestampFromTimeMillis;

/**
 * Contains factory methods for MongoDB sources.
 * <p>
 * See {@link MongoSourceBuilder} for creating custom MongoDB sources.
 *
 * @since 5.3
 */
public final class MongoSources {

    private MongoSources() {
    }

    /**
     * Creates as builder for new batch mongo source. Equivalent to calling {@link MongoSourceBuilder#batch}.
     * <p>
     * Example usage:
     * <pre>{@code
     * BatchSource<MyDTO> batchSource =
     *         MongoSources.batch(() -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *                 .database("myDatabase")
     *                 .collection("myCollection", MyDTO.class)
     *                 .filter(new Document("age", new Document("$gt", 10)),
     *                 .projection(new Document("age", 1))
     *         );
     * Pipeline p = Pipeline.create();
     * BatchStage<Document> srcStage = p.readFrom(batchSource);
     * }</pre>
     *
     * @since 5.3
     * @param clientSupplier a function that creates MongoDB client.
     * @return Batch Mongo source builder
     */
    @Nonnull
    public static MongoSourceBuilder.Batch<Document> batch(@Nonnull SupplierEx<? extends MongoClient> clientSupplier) {
        return MongoSourceBuilder.batch(clientSupplier);
    }

    /**
     * Creates as builder for new batch mongo source. Equivalent to calling {@link MongoSourceBuilder#batch}.
     * <p>
     * Example usage:
     * <pre>{@code
     * BatchSource<Document> batchSource =
     *         MongoSources.batch(dataConnectionRef("mongo"))
     *                 .database("myDatabase")
     *                 .collection("myCollection")
     *                 .filter(new Document("age", new Document("$gt", 10)),
     *                 .projection(new Document("age", 1))
     *         );
     * Pipeline p = Pipeline.create();
     * BatchStage<Document> srcStage = p.readFrom(batchSource);
     * }</pre>
     *
     * Connector will use provided data connection reference to obtain an instance of {@link MongoClient}. Depending
     * on the configuration this client may be shared between processors or not.
     *
     * @since 5.3
     * @param dataConnectionRef a reference to mongo data connection
     * @return Batch Mongo source builder
     */
    @Nonnull
    public static MongoSourceBuilder.Batch<Document> batch(@Nonnull DataConnectionRef dataConnectionRef) {
        return MongoSourceBuilder.batch(dataConnectionRef);
    }

    /**
     * Returns a MongoDB batch source which queries the collection using given
     * {@code filter} and applies the given {@code projection} on the documents.
     * <p>
     * See {@link MongoSourceBuilder} for creating custom MongoDB sources.
     * <p>
     * Here's an example which queries documents in a collection having the
     * field {@code age} with a value greater than {@code 10} and applies a
     * projection so that only the {@code age} field is returned in the
     * emitted document.
     *
     * <pre>{@code
     * BatchSource<Document> batchSource =
     *         MongoSources.batch(
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
     * @since 5.3
     *
     * @param connectionString a connection string URI to MongoDB for example:
     *                         {@code mongodb://127.0.0.1:27017}
     * @param database         the name of the database
     * @param collection       the name of the collection
     * @param filter           filter object as a {@link Document}
     * @param projection       projection object as a {@link Document}
     */
    @Nonnull
    public static BatchSource<Document> batch(
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Bson filter,
            @Nullable Bson projection
    ) {
        String name = name(database, collection);
        Batch<Document> builder = MongoSourceBuilder
                .batch(name, () -> MongoClients.create(connectionString))
                .database(database)
                .collection(collection);
        if (projection != null) {
            builder.project(projection);
        }
        if (filter != null) {
            builder.filter(filter);
        }
        return builder.build();
    }

    /**
     * Returns a MongoDB batch source which queries the collection using given
     * {@code filter} and applies the given {@code projection} on the documents.
     * <p>
     * See {@link MongoSourceBuilder} for creating custom MongoDB sources.
     * <p>
     * Here's an example which queries documents in a collection having the
     * field {@code age} with a value greater than {@code 10} and applies a
     * projection so that only the {@code age} field is returned in the
     * emitted document.
     *
     * <pre>{@code
     * BatchSource<Document> batchSource =
     *         MongoSources.batch(
     *                 dataConnectionRef("mongoDb"),
     *                 "myDatabase",
     *                 "myCollection",
     *                 new Document("age", new Document("$gt", 10)),
     *                 new Document("age", 1)
     *         );
     * Pipeline p = Pipeline.create();
     * BatchStage<Document> srcStage = p.readFrom(batchSource);
     * }</pre>
     * <p>
     * Connector will use provided data connection reference to obtain an instance of {@link MongoClient}. Depending
     * on the configuration this client may be shared between processors or not.
     *
     * @param dataConnectionRef a reference to some mongo data connection
     * @param database          the name of the database
     * @param collection        the name of the collection
     * @param filter            filter object as a {@link Document}
     * @param projection        projection object as a {@link Document}
     * @since 5.3
     */
    @Nonnull
    public static BatchSource<Document> batch(
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Bson filter,
            @Nullable Bson projection
    ) {
        String name = name(database, collection);
        Batch<Document> builder = MongoSourceBuilder
                .batch(name, dataConnectionRef)
                .database(database)
                .collection(collection);
        if (projection != null) {
            builder.project(projection);
        }
        if (filter != null) {
            builder.filter(filter);
        }
        return builder.build();
    }

    /**
     * Creates as builder for new stream mongo source. Equivalent to calling {@link MongoSourceBuilder#stream}.
     *
     * Example usage:
     * <pre>{@code
     * StreamSource<Document> streamSource =
     *         MongoSources.stream(() -> MongoClients.create("mongodb://127.0.0.1:27017"))
     *                 .database("myDatabase")
     *                 .collection("myCollection")
     *                 .filter(new Document("fullDocument.age", new Document("$gt", 10)),
     *                 .projection(new Document("fullDocument.age", 1))
     *         );
     * Pipeline p = Pipeline.create();
     * StreamStage<Document> srcStage = p.readFrom(streamSource);
     * }</pre>
     *
     * @since 5.3
     *
     * @param clientSupplier a function that creates MongoDB client.
     * @return Stream Mongo source builder
     */
    @Nonnull
    public static MongoSourceBuilder.Stream<Document> stream(
            @Nonnull SupplierEx<? extends MongoClient> clientSupplier) {
        return MongoSourceBuilder.stream(clientSupplier);
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
     * See {@link MongoSourceBuilder} for creating custom MongoDB sources.
     * <p>
     * Here's an example which streams inserts on a collection having the
     * field {@code age} with a value greater than {@code 10} and applies a
     * projection so that only the {@code age} field is returned in the
     * emitted document.
     *
     * <pre>{@code
     * StreamSource<? extends Document> streamSource =
     *         MongoSources.stream(
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
     * @since 5.3
     *
     * @param connectionString a connection string URI to MongoDB for example:
     *                         {@code mongodb://127.0.0.1:27017}
     * @param database         the name of the database
     * @param collection       the name of the collection
     * @param filter           filter object as a {@link Document}
     * @param projection       projection object as a {@link Document}
     */
    @Nonnull
    public static StreamSource<? extends Document> stream(
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection,
            @Nullable Document filter,
            @Nullable Document projection
    ) {
        String name = name(database, collection);
        Stream<Document> builder = MongoSourceBuilder
                .stream(name, () -> MongoClients.create(connectionString))
                .database(database)
                .collection(collection)
                .mapFn((d, t) -> d.getFullDocument());
        if (projection != null) {
            builder.project(projection);
        }
        if (filter != null) {
            builder.filter(filter);
        }
        builder.startAtOperationTime(bsonTimestampFromTimeMillis(System.currentTimeMillis()));
        return builder.build();
    }

    private static String name(String database, String collection) {
        return "MongoSource(" + database + "/" + collection + ")";
    }
}
