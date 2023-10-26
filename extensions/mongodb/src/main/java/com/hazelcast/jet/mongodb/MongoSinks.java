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
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Sink;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for MongoDB sinks.
 *
 * @since 5.3
 */
public final class MongoSinks {

    private MongoSinks() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link Sink} for the Pipeline API.
     * <p>
     * The sink inserts or replaces the items it receives to specified collection using
     * {@link MongoCollection#bulkWrite}.
     * <p>
     * All operations are done within transaction if processing guarantee
     * of the job is {@link com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE}.
     * <p>
     * All writes are done using default MongoDB codecs with POJO class codec added.
     *<p>
     * Example usage:
     * <pre>{@code
     * Sink<Document> mongoSink =
     *         MongoSinks.builder(
     *                     Document.class,
     *                     () -> MongoClients.create("mongodb://127.0.0.1:27017")
     *                 )
     *                 .into("myDatabase", "myCollection")
     *                 .identifyDocumentBy("_id", doc -> doc.get("_id"))
     *                 .build()
     *         );
     *
     * Pipeline p = Pipeline.create();
     * (...)
     * someStage.writeTo(mongoSink);
     * }</pre>
     * @since 5.3
     *
     * @param clientSupplier MongoDB client supplier
     * @param itemClass          type of document that will be saved
     * @param <T>                type of the items the sink accepts
     */
    public static <T> MongoSinkBuilder<T> builder(
            @Nonnull Class<T> itemClass,
            @Nonnull SupplierEx<MongoClient> clientSupplier
    ) {
        String name = "MongoSink(" + itemClass.getSimpleName() + ")";
        return new MongoSinkBuilder<>(name, itemClass, clientSupplier);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link Sink} for the Pipeline API.
     * <p>
     * The sink inserts or replaces the items it receives to specified collection using
     * {@link MongoCollection#bulkWrite}.
     * <p>
     * All operations are done within transaction if processing guarantee
     * of the job is {@link com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE}.
     * <p>
     * All writes are done using default MongoDB codecs with POJO class codec added.
     * <p>
     * Example usage:
     * <pre>{@code
     * Sink<Document> mongoSink =
     *         MongoSinks.builder(
     *                     Document.class,
     *                     dataConnectionRef("someMongoDB")
     *                 )
     *                 .into("myDatabase", "myCollection")
     *                 .identifyDocumentBy("_id", doc -> doc.get("_id"))
     *                 .build()
     *         );
     *
     * Pipeline p = Pipeline.create();
     * (...)
     * someStage.writeTo(mongoSink);
     * }</pre>
     * <p>
     * Connector will use provided data connection reference to obtain an instance of {@link MongoClient}. Depending
     * on the configuration this client may be shared between processors or not.
     *
     * @param dataConnectionRef reference to mongo data connection
     * @param itemClass         type of document that will be saved
     * @param <T>               type of the items the sink accepts
     * @since 5.3
     */
    public static <T> MongoSinkBuilder<T> builder(
            @Nonnull Class<T> itemClass,
            @Nonnull DataConnectionRef dataConnectionRef
            ) {
        String name = "MongoSink(" + itemClass.getSimpleName() + ")";
        return new MongoSinkBuilder<>(name, itemClass, dataConnectionRef);
    }

    /**
     * Convenience for {@link #builder}.
     *
     * Example usage:
     * <pre>{@code
     * Sink<Document> mongoSink =
     *         MongoSinks.mongodb(
     *                 "mongodb://127.0.0.1:27017",
     *                 "myDatabase",
     *                 "myCollection"
     *         );
     *
     * Pipeline p = Pipeline.create();
     * (...)
     * someStage.writeTo(mongoSink);
     * }</pre>
     *
     * @since 5.3
     *
     * @param connectionString connection string to MongoDB instance
     * @param database database to which the documents will be put into
     * @param collection collection to which the documents will be put into
     */
    public static Sink<Document> mongodb(
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection
    ) {
        String name = "MongoSink(" + database + "/" + collection + ")";
        return new MongoSinkBuilder<>(name, Document.class, () -> MongoClients.create(connectionString))
                .into(database, collection)
                .identifyDocumentBy("_id", doc -> doc.get("_id"))
                .build();
    }

    /**
     * Convenience for {@link #builder}.
     *
     * Example usage:
     * <pre>{@code
     * Sink<Document> mongoSink =
     *         MongoSinks.mongodb(
     *                 dataConnectionRef("someMongoDB"),
     *                 "myDatabase",
     *                 "myCollection"
     *         );
     *
     * Pipeline p = Pipeline.create();
     * (...)
     * someStage.writeTo(mongoSink);
     * }</pre>
     *
     * @since 5.3
     *
     * @param dataConnectionRef reference to some mongo data connection
     * @param database database to which the documents will be put into
     * @param collection collection to which the documents will be put into
     */
    public static Sink<Document> mongodb(
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull String database,
            @Nonnull String collection
    ) {
        String name = "MongoSink(ref " + dataConnectionRef + ")";
        return new MongoSinkBuilder<>(name, Document.class, dataConnectionRef)
                .into(database, collection)
                .identifyDocumentBy("_id", doc -> doc.get("_id"))
                .build();
    }


}
