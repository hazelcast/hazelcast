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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.impl.RetryTracker;
import com.hazelcast.logging.ILogger;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.mongodb.MongoUtilities.partitionAggregate;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Sorts.ascending;

/**
 * Processor for reading from MongoDB
 *
 * TODO add more info
 * @param <I> type of emitted item
 */
public class ReadMongoP<I> extends AbstractProcessor {

    private static final long RETRY_INTERVAL = 3_000;
    private static final int BATCH_SIZE = 1000;
    private ILogger logger;

    private int totalParallelism;
    private int processorIndex;

    private boolean snapshotsEnabled;

    private boolean snapshotInProgress;
    private final MongoChunkedReader reader;

    private Traverser<?> traverser;
    private Traverser<? extends Entry<BroadcastKey<Integer>, ?>> snapshotTraverser;
    public ReadMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            List<Bson> aggregates,
            String databaseName,
            String collectionName,
            FunctionEx<Document, I> mapItemFn
    ) {
        reader = new BatchMongoTraverser(connectionSupplier, databaseName, collectionName, mapItemFn, aggregates);
    }

    public ReadMongoP(
            SupplierEx<? extends MongoClient> connectionSupplier,
            Long startAtTimestamp,
            EventTimePolicy<? super I> eventTimePolicy,
            List<Bson> aggregates,
            String databaseName,
            String collectionName,
            FunctionEx<ChangeStreamDocument<Document>, I> mapStreamFn
    ) {
        EventTimeMapper<I> eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
        reader = new StreamMongoTraverser(connectionSupplier, databaseName, collectionName, mapStreamFn,
                startAtTimestamp, aggregates, eventTimeMapper);
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        totalParallelism = context.totalParallelism();
        processorIndex = context.globalProcessorIndex();
        this.snapshotsEnabled = context.snapshottingEnabled();

        reader.reconnectIfNecessary(snapshotsEnabled);
    }

    @Override
    public boolean complete() {
        reader.reconnectIfNecessary(snapshotsEnabled);
        if (traverser == null) {
            this.traverser = reader.nextChunkTraverser().onFirstNull(() -> traverser = null);
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        if (snapshotInProgress) {
            return false;
        }

        return reader.everCompletes();
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotsEnabled) {
            return true;
        }
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            int partition = processorIndex % totalParallelism;
            snapshotTraverser = singleton(entry(broadcastKey(partition), reader.snapshot()))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot.");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        snapshotInProgress = false;
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int keyInteger = (int) key;
        if (keyInteger % totalParallelism == processorIndex) {
            reader.restore(value);
        }
    }

    private abstract class MongoChunkedReader {
        private final RetryTracker connectionRetryTracker;
        private final SupplierEx<? extends MongoClient> connectionSupplier;
        private final String databaseName;
        private final String collectionName;

        protected MongoClient mongoClient;
        protected MongoDatabase database;
        protected MongoCollection<Document> collection;

        protected MongoChunkedReader(String databaseName, String collectionName, SupplierEx<? extends MongoClient> connectionSupplier) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.connectionSupplier = connectionSupplier;
            this.connectionRetryTracker = new RetryTracker(RetryStrategies.indefinitely(RETRY_INTERVAL));
        }

        void reconnectIfNecessary(boolean snapshotsEnabled) {
            if (!isConnectionUp()) {
                if (connectionRetryTracker.shouldTryAgain()) {
                    connect(snapshotsEnabled);
                } else {
                    throw new JetException("cannot connect to MongoDB");
                }
            }
        }

        private boolean isConnectionUp() {
            return mongoClient != null;
        }

        void onConnect(MongoClient mongoClient, boolean snapshotsEnabled) {
        }

        private void connect(boolean snapshotsEnabled) {
            try {
                mongoClient = connectionSupplier.get();
                if (databaseName != null) {
                    this.database = mongoClient.getDatabase(databaseName);
                }
                if (collectionName != null) {
                    checkState(databaseName != null, "you have to provide database name if collection name" +
                            " is specified");
                    //noinspection ConstantValue false warn by intellij
                    checkState(database != null, "database " + databaseName + " does not exists");
                    this.collection = database.getCollection(collectionName);
                }

                onConnect(mongoClient, snapshotsEnabled);

                connectionRetryTracker.reset();
            } catch (Exception e) {
                logger.warning("Could not connect to MongoDB", e);
                connectionRetryTracker.attemptFailed();
            }
        }

        @Nonnull
        abstract Traverser<?> nextChunkTraverser();

        @Nonnull
        abstract Object snapshot();

        abstract void restore(Object value);

        void close() {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        abstract boolean everCompletes();
    }

    private final class BatchMongoTraverser extends MongoChunkedReader {
        private final FunctionEx<Document, I> mapItemFn;
        private final List<Bson> aggregates;
        private Traverser<Document> delegate;
        private ObjectId lastKey;

        private BatchMongoTraverser(
                SupplierEx<? extends MongoClient> connectionSupplier,
                String databaseName,
                String collectionName,
                FunctionEx<Document, I> mapItemFn,
                List<Bson> aggregates) {
            super(databaseName, collectionName, connectionSupplier);
            this.mapItemFn = mapItemFn;
            this.aggregates = aggregates;
        }

        @Override
        void onConnect(MongoClient mongoClient, boolean supportsSnapshots) {
            List<Bson> aggregateList = new ArrayList<>(aggregates);
            if (supportsSnapshots) {
                aggregateList.add(sort(ascending("_id")).toBsonDocument());
            }
            if (totalParallelism > 1) {
                aggregateList.addAll(0, partitionAggregate(totalParallelism, processorIndex, false));
            }
            if (collection != null) {
                this.delegate = delegateForCollection(collection, aggregateList);
            }
            else if (database != null) {
                this.delegate = delegateForDb(database, aggregateList);
            }
            else {
                final MongoClient clientLocal = mongoClient;
                this.delegate = traverseIterable(mongoClient.listDatabaseNames())
                        .flatMap(name -> {
                            MongoDatabase db = clientLocal.getDatabase(name);
                            return delegateForDb(db, aggregateList);
                        });
            }
            checkNotNull(this.delegate, "unable to construct Mongo traverser");
        }

        private Traverser<Document> delegateForCollection(MongoCollection<Document> collection, List<Bson> aggregateList) {
            return traverseIterable(collection.aggregate(aggregateList));
        }

        private Traverser<Document> delegateForDb(MongoDatabase database, List<Bson> aggregateList) {
            MongoIterable<String> collectionsIterable = database.listCollectionNames();

            return traverseIterable(collectionsIterable)
                    .flatMap(colName -> delegateForCollection(database.getCollection(colName), aggregateList));
        }

        @Nonnull
        @Override
        public Traverser<I> nextChunkTraverser() {
            List<I> chunk = new ArrayList<>(BATCH_SIZE);
            Document doc;
            Document lastItem = null;
            while ((doc = delegate.next()) != null) {
                chunk.add(mapItemFn.apply(doc));
                lastItem = doc;
            }
            lastKey = lastItem == null ? null : lastItem.getObjectId("__id");

            return Traversers.traverseIterable(chunk);
        }

        @Override
        boolean everCompletes() {
            return true;
        }

        @Nonnull
        @Override
        public Object snapshot() {
            return lastKey;
        }

        @Override
        public void restore(Object value) {
            lastKey = (ObjectId) value;
        }

        @Override
        public void close() {

        }
    }

    private final class StreamMongoTraverser extends MongoChunkedReader {
        private ChangeStreamIterable<Document>  changeStream;
        private final FunctionEx<ChangeStreamDocument<Document>, I> mapFn;
        private final Long startTimestamp;
        private final List<Bson> aggregates;
        private final EventTimeMapper<I> eventTimeMapper;
        private MongoCursor<ChangeStreamDocument<Document>> cursor;
        private BsonDocument resumeToken;

        private Traverser<Object> traverser;

        private StreamMongoTraverser(
                SupplierEx<? extends MongoClient> connectionSupplier,
                String databaseName,
                String collectionName,
                FunctionEx<ChangeStreamDocument<Document>, I> mapFn,
                Long startTimestamp,
                List<Bson> aggregates,
                EventTimeMapper<I> eventTimeMapper
        ) {
            super(databaseName, collectionName, connectionSupplier);
            this.mapFn = mapFn;
            this.startTimestamp = startTimestamp;
            this.aggregates = aggregates;
            this.eventTimeMapper = eventTimeMapper;
        }

        @Override
        public void onConnect(MongoClient mongoClient, boolean snapshotsEnabled) {
            List<Bson> aggregateList = new ArrayList<>(aggregates);
            if (totalParallelism > 1) {
                aggregateList.addAll(0, partitionAggregate(totalParallelism, processorIndex, true));
            }
            if (collection != null) {
                this.changeStream = collection.watch(aggregateList);
            } else if (database != null) {
                this.changeStream = database.watch(aggregateList);
            } else {
                this.changeStream = mongoClient.watch(aggregateList);
            }
        }

        @Override
        boolean everCompletes() {
            return false;
        }

        @Nonnull
        @Override
        public Traverser<?> nextChunkTraverser() {
            try {
                if (cursor == null) {
                    if (resumeToken != null) {
                        changeStream.resumeAfter(resumeToken);
                    }
                    else if (startTimestamp != null) {
                        changeStream.startAtOperationTime(new BsonTimestamp(startTimestamp));
                    }
                    cursor = changeStream.batchSize(BATCH_SIZE).iterator();
                }

                ArrayList<ChangeStreamDocument<Document>> chunk = new ArrayList<>(BATCH_SIZE);
                ChangeStreamDocument<Document> document = null;
                int count = 0;
                boolean eagerEnd = false;
                while (count < BATCH_SIZE && !eagerEnd) {
                    ChangeStreamDocument<Document> doc = cursor.tryNext();
                    if (doc != null) {
                        document = doc;
                        chunk.add(document);
                        count++;
                    } else {
                        eagerEnd = true;
                    }
                }
                resumeToken = document == null ? null : document.getResumeToken();

                traverser = Traversers.traverseIterable(chunk)
                                      .onFirstNull(() -> {
                                          traverser = null;
                                      })
                                      .flatMap(doc -> {
                                          long eventTime = clusterTime(doc);
                                          I item = mapFn.apply(doc);
                                          return item == null
                                                  ? Traversers.empty()
                                                  : eventTimeMapper.flatMapEvent(item, 0, eventTime);
                                      });
            } catch (MongoException e) {
                throw new JetException("error while reading from mongodb", e);
            }
            return traverser;
        }

        private long clusterTime(ChangeStreamDocument<Document> changeStreamDocument) {
            BsonTimestamp clusterTime = changeStreamDocument.getClusterTime();
            return clusterTime == null ? System.currentTimeMillis() : clusterTime.getValue();
        }

        @Nonnull
        @Override
        public Object snapshot() {
            return resumeToken;
        }

        @Override
        public void restore(Object value) {
            this.resumeToken = (BsonDocument) value;
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }
        }
    }
}
