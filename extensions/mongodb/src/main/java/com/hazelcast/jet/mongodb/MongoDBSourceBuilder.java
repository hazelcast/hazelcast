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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.mongodb.impl.ReadMongoP;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.spi.annotation.Beta;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.mongodb.impl.Mappers.streamToClass;
import static com.hazelcast.jet.mongodb.impl.Mappers.toClass;

/**
 * Top-level class for MongoDB custom source builders.
 * <p>
 * For details refer to the factory methods:
 * <ul>
 *      <li>{@link #batch(String, SupplierEx)}</li>
 *      <li>{@link #stream(String, SupplierEx)}</li>
 * </ul>
 *
 * @since 5.3
 *
 */
@Beta
public final class MongoDBSourceBuilder {

    private MongoDBSourceBuilder() {
    }


    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link BatchSource} for the Pipeline API.
     * <p>
     * The created source will not be distributed, a single processor instance
     * will be created on an arbitrary member.
     * <p>
     * Here's an example that builds a simple source which queries all the
     * documents in a collection and emits the items as a string by transforming
     * each item to json.
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param clientSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder.Batch<Document> batch(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> clientSupplier
    ) {
        return new Batch<>(name, clientSupplier);
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * a custom MongoDB {@link StreamSource} for the Pipeline API.
     * <p>
     * The created source will not be distributed, a single processor instance
     * will be created on an arbitrary member. The source provides native
     * timestamps using {@link ChangeStreamDocument#getClusterTime()} and fault
     * tolerance using {@link ChangeStreamDocument#getResumeToken()}.
     *
     * @param name               a descriptive name for the source (diagnostic purposes)
     * @param clientSupplier a function that creates MongoDB client
     */
    @Nonnull
    public static MongoDBSourceBuilder.Stream<Document> stream(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends MongoClient> clientSupplier
    ) {
        return new Stream<>(name, clientSupplier);
    }

    private abstract static class Base<T> {

        protected String name;
        protected SupplierEx<? extends MongoClient> clientSupplier;
        protected Class<T> mongoType;

        protected String databaseName;
        protected String collectionName;
        protected final List<Bson> aggregates = new ArrayList<>();

        @Nonnull
        public Base<T> database(String database) {
            databaseName = database;
            return this;
        }

        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Base<T_NEW> collection(String collectionName, Class<T_NEW> mongoType) {
            Base<T_NEW> newThis = (Base<T_NEW>) this;
            newThis.collectionName = collectionName;
            newThis.mongoType = mongoType;
            return newThis;
        }

        @Nonnull
        public Base<Document> collection(String collectionName) {
            return this.collection(collectionName, Document.class);
        }
    }

    /**
     * See {@link #batch(String, SupplierEx)}.
     *
     * @param <T> type of the emitted objects
     */
    public static final class Batch<T> extends Base<T> {

        private FunctionEx<Document, T> mapFn;

        @SuppressWarnings("unchecked")
        private Batch(
                @Nonnull String name,
                @Nonnull SupplierEx<? extends MongoClient> clientSupplier
        ) {
            checkSerializable(clientSupplier, "clientSupplier");
            this.name = name;
            this.clientSupplier = clientSupplier;
            mapFn = (FunctionEx<Document, T>) toClass(Document.class);
        }

        /**
         * Adds a projection aggregate. Example use:
         * <pre>{@code
         * import static com.mongodb.client.model.Projections.include;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .projection(include("fieldName"));
         * }</pre>
         * @param projection Bson form of projection;
         *                   use {@link com.mongodb.client.model.Projections} to create projection.
         * @return this builder with projection added
         */
        @Nonnull
        public Batch<T> project(@Nonnull Bson projection) {
            aggregates.add(Aggregates.project(projection).toBsonDocument());
            return this;
        }

        /**
         * Adds sort aggregate to this builder.
         *
         * Example usage:
         * <pre>{@code
         *  import static com.mongodb.client.model.Sorts.ascending;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .sort(ascending("fieldName"));
         * }</pre>
         * @param sort Bson form of sort. Use {@link com.mongodb.client.model.Sorts} to create sort.
         * @return this builder with aggregate added
         */
        @Nonnull
        public Batch<T> sort(@Nonnull Bson sort) {
            aggregates.add(Aggregates.sort(sort).toBsonDocument());
            return this;
        }

        /**
         * Adds filter aggregate to this builder, which allows to filter documents in MongoDB, without
         * the need to download all documents.
         *
         * Example usage:
         * <pre>{@code
         *  import static com.mongodb.client.model.Filters.eq;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .filter(eq("fieldName", 10));
         * }</pre>
         * @param filter Bson form of filter. Use {@link com.mongodb.client.model.Filters} to create sort.
         * @return this builder with aggregate added
         */
        @Nonnull
        public Batch<T> filter(@Nonnull Bson filter) {
            checkNotNull(filter, "filter argument cannot be null");
            aggregates.add(Aggregates.match(filter).toBsonDocument());
            return this;
        }

        /**
         * @param mapFn   transforms the queried document to the desired output
         *                object
         * @param <T_NEW> type of the emitted object
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Batch<T_NEW> mapFn(@Nonnull FunctionEx<Document, T_NEW> mapFn) {
            checkNotNull(mapFn, "mapFn argument cannot be null");
            checkSerializable(mapFn, "mapFn must be serializable");
            Batch<T_NEW> newThis = (Batch<T_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        /**
         * Specifies which database will be queried. If not specified, connector will look at all databases.
         * @param database database name to query.
         * @return this builder
         */
        @Override @Nonnull
        public Batch<T> database(@Nullable String database) {
            return (Batch<T>) super.database(database);
        }

        /**
         * Specifies from which collection connector will read documents. If not invoked,
         * then connector will look at all collections in given database.
         *
         * Example usage:
         * <pre>{@code
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection");
         * }</pre>
         *
         * This function is an equivalent of calling {@linkplain #collection(String, Class)} with {@linkplain Document}
         * as the second argument.
         *
         * @param collectionName Name of the collection that will be queried.
         * @return this builder
         */
        @Override @Nonnull
        public Batch<Document> collection(@Nullable String collectionName) {
            return (Batch<Document>) super.collection(collectionName, Document.class);
        }

        /**
         * Specifies from which collection connector will read documents. If not invoked,
         * then connector will look at all collections in given database. All documents read will be automatically
         * parsed to user-defined type using
         * {@linkplain com.mongodb.MongoClientSettings#getDefaultCodecRegistry mongo's standard codec registry}
         * with pojo support added.
         *
         * Example usage:
         * <pre>{@code
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection", MyDocumentPojo.class);
         * }</pre>
         *
         * This function is an equivalent for calling:
         * <pre>{@code
         * import static com.hazelcast.jet.mongodb.impl.Mappers.toClass;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection")
         *      .mapFn(toClass(MyuDocumentPojo.class));
         * }</pre>
         * @param collectionName Name of the collection that will be queried.
         * @param mongoType user defined type to which the document will be parsed.
         * @return this builder
         */
        @Override @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Batch<T_NEW> collection(String collectionName, @Nonnull Class<T_NEW> mongoType) {
            Batch<T_NEW> newThis = (Batch<T_NEW>) this;
            newThis.collection(collectionName);
            newThis.mapFn = toClass(mongoType);
            return newThis;
        }

        /**
         * Creates and returns the MongoDB {@link BatchSource}.
         */
        @Nonnull
        public BatchSource<T> build() {
            checkNotNull(clientSupplier, "clientSupplier must be set");
            checkNotNull(mapFn, "mapFn must be set");

            List<Bson> aggregates = this.aggregates;
            String databaseName = this.databaseName;
            String collectionName = this.collectionName;
            SupplierEx<? extends MongoClient> localclientSupplier = clientSupplier;
            FunctionEx<Document, T> localMapFn = mapFn;

            return Sources.batchFromProcessor(name, ProcessorMetaSupplier.of(
                    () -> new ReadMongoP<>(localclientSupplier, aggregates,
                            databaseName, collectionName, localMapFn)));
        }
    }

    /**
     * See {@link #stream(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     */
    public static final class Stream<T> extends Base<T> {

        FunctionEx<ChangeStreamDocument<Document>, T> mapFn;
        Long startAtOperationTime;

        @SuppressWarnings("unchecked")
        private Stream(
                @Nonnull String name,
                @Nonnull SupplierEx<? extends MongoClient> clientSupplier
        ) {
            checkSerializable(clientSupplier, "clientSupplier");
            this.name = name;
            this.clientSupplier = clientSupplier;
            mapFn = (FunctionEx<ChangeStreamDocument<Document>, T>) streamToClass(Document.class);
        }

        /**
         * Adds a projection aggregate. Example use:
         * <pre>{@code
         * import static com.mongodb.client.model.Projections.include;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .projection(include("fieldName"));
         * }</pre>
         * @param projection Bson form of projection;
         *                   use {@link com.mongodb.client.model.Projections} to create projection.
         * @return this builder with projection added
         */
        @Nonnull
        public Stream<T> project(@Nonnull Bson projection) {
            aggregates.add(Aggregates.project(projection).toBsonDocument());
            return this;
        }

        /**
         * Adds filter aggregate to this builder, which allows to filter documents in MongoDB, without
         * the need to download all documents.
         *
         * Example usage:
         * <pre>{@code
         *  import static com.mongodb.client.model.Filters.eq;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .filter(eq("fieldName", 10));
         * }</pre>
         * @param filter Bson form of filter. Use {@link com.mongodb.client.model.Filters} to create sort.
         * @return this builder with aggregate added
         */
        @Nonnull
        public Stream<T> filter(@Nonnull Bson filter) {
            aggregates.add(Aggregates.match(filter).toBsonDocument());
            return this;
        }

        /**
         * Specifies which database will be queried. If not specified, connector will look at all databases.
         * @param database database name to query.
         * @return this builder
         */
        @Nonnull @Override
        public Stream<T> database(@Nullable String database) {
            databaseName = database;
            return this;
        }

        /**
         * Specifies from which collection connector will read documents. If not invoked,
         * then connector will look at all collections in given database.
         *
         * Example usage:
         * <pre>{@code
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection");
         * }</pre>
         *
         * This function is an equivalent of calling {@linkplain #collection(String, Class)} with {@linkplain Document}
         * as the second argument.
         *
         * @param collectionName Name of the collection that will be queried.
         * @return this builder
         */
        @Nonnull @Override
        public Stream<Document> collection(@Nullable String collectionName) {
            return collection(collectionName, Document.class);
        }

        /**
         * Specifies from which collection connector will read documents. If not invoked,
         * then connector will look at all collections in given database. All documents read will be automatically
         * parsed to user-defined type using
         * {@linkplain com.mongodb.MongoClientSettings#getDefaultCodecRegistry mongo's standard codec registry}
         * with pojo support added.
         *
         * Example usage:
         * <pre>{@code
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection", MyDocumentPojo.class);
         * }</pre>
         *
         * This function is an equivalent for calling:
         * <pre>{@code
         * import static com.hazelcast.jet.mongodb.impl.Mappers.toClass;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .collection("myCollection")
         *      .mapFn(toClass(MyuDocumentPojo.class));
         * }</pre>
         * @param collectionName Name of the collection that will be queried.
         * @param mongoType user defined type to which the document will be parsed.
         * @return this builder
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        @Override
        public <T_NEW> Stream<T_NEW> collection(@Nullable String collectionName, @Nonnull Class<T_NEW> mongoType) {
            Stream<T_NEW> newThis = (Stream<T_NEW>) this;
            newThis.collectionName = collectionName;
            newThis.mapFn = streamToClass(mongoType);
            return newThis;
        }

        /**
         * @param mapFn   transforms the queried document to the desired output
         *                object
         * @param <T_NEW> type of the emitted object
         * @return this builder
         */
        @Nonnull
        @SuppressWarnings("unchecked")
        public <T_NEW> Stream<T_NEW> mapFn(@Nonnull FunctionEx<ChangeStreamDocument<Document>, T_NEW> mapFn) {
            checkSerializable(mapFn, "mapFn");
            Stream<T_NEW> newThis = (Stream<T_NEW>) this;
            newThis.mapFn = mapFn;
            return newThis;
        }

        /**
         * Specifies time from which MongoDB's events will be read.
         *
         * It is <strong>highly</strong> suggested to provide this argument, as it will reduce reading initial
         * state of database.
         * @param startAtOperationTime time from which events should be taken into consideration
         * @return this builder
         */
        @Nonnull
        public Stream<T> startAtOperationTime(@Nonnull BsonTimestamp startAtOperationTime) {
            this.startAtOperationTime = startAtOperationTime.getValue();
            return this;
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * the given collection.
         */
        @Nonnull
        public StreamSource<T> build() {
            checkNotNull(clientSupplier, "clientSupplier must be set");
            checkNotNull(mapFn, "mapFn must be set");

            SupplierEx<? extends MongoClient> localclientSupplier = clientSupplier;
            FunctionEx<ChangeStreamDocument<Document>, T> localMapFn = mapFn;
            Long startAtOperationTime = this.startAtOperationTime;
            List<Bson> aggregates = this.aggregates;
            String databaseName = this.databaseName;
            String collectionName = this.collectionName;

            return Sources.streamFromProcessorWithWatermarks(name, true,
                    eventTimePolicy -> ProcessorMetaSupplier.of(
                        () -> new ReadMongoP<>(localclientSupplier, startAtOperationTime, eventTimePolicy, aggregates,
                                databaseName, collectionName, localMapFn)));
        }
    }

}
