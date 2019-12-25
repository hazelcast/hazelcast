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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.List;

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
     * {@link MongoCollection#insertMany(List, InsertManyOptions)}.
     *
     * <p>
     * These are the callback functions you can provide to implement the sink's
     * behavior:
     * <ol><li>
     *     {@code connectionSupplier} supplies MongoDb client. It will be called
     *     once for each worker thread. This component is required.
     * </li><li>
     *     {@code databaseFn} creates/obtains a database using the given client.
     *     It will be called once for each worker thread. This component is
     *     required.
     * </li><li>
     *     {@code collectionFn} creates/obtains a collection in the given
     *     database. It will be called once for each worker thread. This
     *     component is required.
     * </li><li>
     *     {@code destroyFn} destroys the client. It will be called upon
     *     completion to release any resource. This component is optional.
     * </li><li>
     *     {@code ordered} sets {@link InsertManyOptions#ordered(boolean)}.
     *     Defaults to {@code true}.
     * </li><li>
     *     {@code bypassValidation} sets {@link
     *     InsertManyOptions#bypassDocumentValidation(Boolean)}. Defaults to
     *     {@code false}.
     * </li><li>
     *     {@code preferredLocalParallelism} sets the local parallelism of the
     *     sink. See {@link SinkBuilder#preferredLocalParallelism(int)} for more
     *     information. Defaults to {@code 2}.
     * </li></ol>
     *
     * @param name               name of the sink
     * @param connectionSupplier MongoDB client supplier
     * @param <T>                type of the items the sink accepts
     */
    public static <T> MongoDBSinkBuilder<T> builder(
            @Nonnull String name,
            @Nonnull SupplierEx<MongoClient> connectionSupplier
    ) {
        return new MongoDBSinkBuilder<>(name, connectionSupplier);
    }


    /**
     * Convenience for {@link #builder(String, SupplierEx)}.
     */
    public static Sink<Document> mongodb(
            @Nonnull String name,
            @Nonnull String connectionString,
            @Nonnull String database,
            @Nonnull String collection
    ) {
        return MongoDBSinks
                .<Document>builder(name, () -> MongoClients.create(connectionString))
                .databaseFn(client -> client.getDatabase(database))
                .collectionFn(db -> db.getCollection(collection))
                .destroyFn(MongoClient::close)
                .build();
    }


}
