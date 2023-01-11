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

package com.hazelcast.jet.mongodb;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import javax.annotation.Nonnull;

/**
 * Contains factory methods for MongoDB sinks.
 */
public final class MongoDBSinks {

    private MongoDBSinks() {
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link Sink} for the Pipeline API.
     * <p>
     * The sink inserts the items it receives to specified collection using
     * {@link MongoCollection#updateOne}. Updates are done within transaction if processing guarantee
     * of the job is {@link com.hazelcast.jet.config.ProcessingGuarantee#EXACTLY_ONCE}.
     *
     * @param name               name of the sink
     * @param connectionSupplier MongoDB client supplier
     * @param <T>                type of the items the sink accepts
     */
    public static <T> MongoDBSinkBuilder<T> builder(
            @Nonnull String name,
            @Nonnull Class<T> itemClass,
            @Nonnull SupplierEx<MongoClient> connectionSupplier
    ) {
        return new MongoDBSinkBuilder<>(name, itemClass, connectionSupplier);
    }


    /**
     * Convenience for {@link #builder}.
     */
    public static Sink<Document> mongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection
    ) {
        return MongoDBSinks
                .builder(name, Document.class, () -> MongoClients.create(connectionString))
                .into(database, collection)
                .identifyDocumentBy("_id", doc -> doc.get("_id"))
                .build();
    }


}
