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
import com.hazelcast.jet.mongodb.impl.ReadMongoParams;
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
        protected ReadMongoParams<T> params;

        protected String name;

        @Nonnull
        public Base<T> database(String database) {
            params.setDatabaseName(database);
            return this;
        }

        @Nonnull
        public abstract <T_NEW> Base<T_NEW> collection(String collectionName, Class<T_NEW> mongoType);

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

        @SuppressWarnings("unchecked")
        private Batch(
                @Nonnull String name,
                @Nonnull SupplierEx<? extends MongoClient> clientSupplier
        ) {
            checkSerializable(clientSupplier, "clientSupplier");
            this.name = name;
            this.params = new ReadMongoParams<>(false);
            params
                    .setClientSupplier(clientSupplier)
                    .setMapItemFn((FunctionEx<Document, T>) toClass(Document.class));
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
            params.addAggregate(Aggregates.project(projection).toBsonDocument());
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
            params.addAggregate(Aggregates.sort(sort).toBsonDocument());
            return this;
        }

        /**
         * Adds filter aggregate to this builder, which allows to filter documents in MongoDB, without
         * the need to download all documents.
         * <p>
         * Example usage:
         * <pre>{@code
         *  import static com.mongodb.client.model.Filters.eq;
         *
         *  MongoDBSourceBuilder.stream(name, supplier)
         *      .filter(eq("fieldName", 10));
         * }</pre>
         *
         * @param filter Bson form of filter. Use {@link com.mongodb.client.model.Filters} to create sort.
         * @return this builder with aggregate added
         */
        @Nonnull
        public Batch<T> filter(@Nonnull Bson filter) {
            checkNotNull(filter, "filter argument cannot be null");
            params.addAggregate(Aggregates.match(filter).toBsonDocument());
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
            Batch<T_NEW> newThis = (Batch<T_NEW>) this;
            newThis.params.setMapItemFn(mapFn);
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
         * <p>
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
            return collection(collectionName, Document.class);
        }

        /**
         * Specifies from which collection connector will read documents. If not invoked,
         * then connector will look at all collections in given database. All documents read will be automatically
         * parsed to user-defined type using
         * {@linkplain com.mongodb.MongoClientSettings#getDefaultCodecRegistry mongo's standard codec registry}
         * with pojo support added.
         * <p>
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
            newThis.params.setCollectionName(collectionName);
            newThis.params.setMapItemFn(toClass(mongoType));
            return newThis;
        }

        /**
         * Creates and returns the MongoDB {@link BatchSource}.
         */
        @Nonnull
        public BatchSource<T> build() {
            checkNotNull(params.getClientSupplier(), "clientSupplier must be set");
            checkNotNull(params.getMapItemFn(), "mapFn must be set");

            final ReadMongoParams<T> localParams = params;

            return Sources.batchFromProcessor(name, ProcessorMetaSupplier.of(
                    () -> new ReadMongoP<>(localParams)));
        }
    }

    /**
     * See {@link #stream(String, SupplierEx)}.
     *
     * @param <T> type of the queried documents
     */
    public static final class Stream<T> extends Base<T> {

        @SuppressWarnings("unchecked")
        private Stream(
                @Nonnull String name,
                @Nonnull SupplierEx<? extends MongoClient> clientSupplier
        ) {
            checkSerializable(clientSupplier, "clientSupplier");
            this.name = name;
            this.params = new ReadMongoParams<>(true);
            this.params.setClientSupplier(clientSupplier);
            this.params.setMapStreamFn((FunctionEx<ChangeStreamDocument<Document>, T>) streamToClass(Document.class));
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
            params.addAggregate(Aggregates.project(projection).toBsonDocument());
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
            params.addAggregate(Aggregates.match(filter).toBsonDocument());
            return this;
        }

        /**
         * Specifies which database will be queried. If not specified, connector will look at all databases.
         * @param database database name to query.
         * @return this builder
         */
        @Nonnull @Override
        public Stream<T> database(@Nullable String database) {
            params.setDatabaseName(database);
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
            newThis.params.setCollectionName(collectionName);
            newThis.params.setMapStreamFn(streamToClass(mongoType));
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
            newThis.params.setMapStreamFn(mapFn);
            return newThis;
        }

        /**
         * Specifies time from which MongoDB's events will be read.
         * <p>
         * It is <strong>highly</strong> suggested to provide this argument, as it will reduce reading initial
         * state of database.
         * @param startAtOperationTime time from which events should be taken into consideration
         * @return this builder
         */
        @Nonnull
        public Stream<T> startAtOperationTime(@Nonnull BsonTimestamp startAtOperationTime) {
            this.params.setStartAtTimestamp(startAtOperationTime.getValue());
            return this;
        }

        /**
         * Creates and returns the MongoDB {@link StreamSource} which watches
         * the given collection.
         */
        @Nonnull
        public StreamSource<T> build() {
            checkNotNull(params.getClientSupplier(), "clientSupplier must be set");
            checkNotNull(params.getMapStreamFn(), "mapFn must be set");

            final ReadMongoParams<T> localParams = params;

            return Sources.streamFromProcessorWithWatermarks(name, true,
                    eventTimePolicy -> ProcessorMetaSupplier.of(
                        () -> new ReadMongoP<>(localParams.setEventTimePolicy(eventTimePolicy))));
        }
    }

}
