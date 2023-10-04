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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.mongodb.impl.CursorTraverser.EmptyItem;
import com.hazelcast.jet.mongodb.impl.ReadMongoParams.Aggregates;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkCollectionExists;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkDatabaseExists;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.partitionAggregate;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;

/**
 * Processor for reading from MongoDB
 * <p>
 * Reading is done by one of two readers:
 * <ul>
 * <li>batch reader, which uses {@linkplain MongoCollection#aggregate} to find matching documents.</li>
 * <li>streaming reader, which uses {@linkplain MongoClient#watch} (or same function on database or collection level)
 * to find matching documents, as they arrive in the stream.</li>
 * <p>
 * All processing guarantees are supported via standard snapshotting mechanism. Each instance of this processor
 * will save it's state (last read key or resumeToken) with the key being global processor index mod total parallelism.
 * </ul>
 *
 * @param <I> type of emitted item
 */
public class ReadMongoP<I> extends AbstractProcessor {

    private static final int BATCH_SIZE = 1000;
    private final boolean checkExistenceOnEachConnect;
    private ILogger logger;

    private int totalParallelism;
    private int processorIndex;

    private boolean snapshotsEnabled;

    private boolean snapshotInProgress;
    private final MongoChunkedReader reader;
    private final MongoConnection connection;
    /**
     * Means that user requested the query to be executed in non-distributed way.
     * This property set to true should mean that
     * {@link com.hazelcast.jet.core.ProcessorMetaSupplier#forceTotalParallelismOne} was used.
     */
    private final boolean nonDistributed;

    private Traverser<?> traverser;
    private Traverser<Entry<BroadcastKey<Integer>, Object>> snapshotTraverser;

    /**
     * Parallelization of reading is possible only when
     * user didn't mark the processor as nonDistributed
     * and when totalParallelism is higher than 1.
     */
    private boolean canParallelize;

    public ReadMongoP(ReadMongoParams<I> params) {
        if (params.isStream()) {
            EventTimeMapper<I> eventTimeMapper = new EventTimeMapper<>(params.eventTimePolicy);
            eventTimeMapper.addPartitions(1);
            this.reader = new StreamMongoReader(params.databaseName, params.collectionName, params.mapStreamFn,
                    params.getStartAtTimestamp(), params.getAggregates(), eventTimeMapper);
        } else {
            this.reader = new BatchMongoReader(params.databaseName, params.collectionName, params.mapItemFn,
                    params.getAggregates());
        }
        this.connection = new MongoConnection(
                params.clientSupplier, params.dataConnectionRef, client -> reader.connect(client, snapshotsEnabled)
        );
        this.nonDistributed = params.isNonDistributed();
        this.checkExistenceOnEachConnect = params.isCheckExistenceOnEachConnect();
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        totalParallelism = context.totalParallelism();
        canParallelize = !nonDistributed && totalParallelism > 1;
        processorIndex = context.globalProcessorIndex();
        this.snapshotsEnabled = context.snapshottingEnabled();

        NodeEngineImpl nodeEngine = Util.getNodeEngine(context.hazelcastInstance());
        connection.assembleSupplier(nodeEngine);

        try {
            connection.reconnectIfNecessary();
        } catch (MongoException e) {
            throw new JetException(e);
        }
    }

    /**
     * Source cannot be cooperative; the async driver is much older than sync driver, probably it's not Mongo's team
     * priority to keep it up to date. Sync version seems to be better for us then.
     */
    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (!connection.reconnectIfNecessary()) {
            return false;
        }
        if (traverser == null) {
            this.traverser = reader.nextChunkTraverser()
                                   .onFirstNull(() -> traverser = null);
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
        closeResource(reader);
        closeResource(connection);
    }

    @Override
    public boolean saveToSnapshot() {
        if (!snapshotsEnabled) {
            return true;
        }
        if (traverser != null && !emitFromTraverser(traverser)) {
            return false;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            int partition = processorIndex % totalParallelism;
            Object snapshot = reader.snapshot();
            if (snapshot == null) {
                return true;
            }
            snapshotTraverser = singleton(entry(broadcastKey(partition), snapshot))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot.");
                    });

            if (reader.supportsWatermarks()) {
                Object watermark = reader.watermark();
                snapshotTraverser = snapshotTraverser.append(entry(broadcastKey(-partition), watermark));
            }
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        if (logger.isFineEnabled()) {
            logger.fine("Snapshot commit finished");
        }
        snapshotInProgress = false;
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        int keyInteger = ((BroadcastKey<Integer>) key).key();
        boolean wm = keyInteger < 0;
        int keyAb = Math.abs(keyInteger);
        boolean forThisProcessor = keyAb % totalParallelism == processorIndex;
        if (forThisProcessor) {
            if (!wm) {
                reader.restore(value);
                reader.connect(connection.client(), true);
            } else if (reader.supportsWatermarks()) {
                reader.restoreWatermark((Long) value);
            }
        }
    }

    private abstract class MongoChunkedReader implements Closeable {

        protected MongoDatabase database;
        protected MongoCollection<Document> collection;
        private final String databaseName;
        private final String collectionName;

        protected MongoChunkedReader(
                String databaseName,
                String collectionName
        ) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
        }

        void onConnect(MongoClient mongoClient, boolean snapshotsEnabled) {
        }

        void connect(MongoClient newClient, boolean snapshotsEnabled) {
            try {
                logger.fine("(Re)connecting to MongoDB");
                if (databaseName != null) {
                    this.database = newClient.getDatabase(databaseName);

                    if (checkExistenceOnEachConnect) {
                        checkDatabaseExists(newClient, databaseName);
                    }
                }
                if (collectionName != null) {
                    checkState(databaseName != null, "you have to provide database name if collection name" +
                            " is specified");
                    checkState(database != null, "database " + databaseName + " does not exists");

                    if (checkExistenceOnEachConnect) {
                        checkCollectionExists(database, collectionName);
                    }
                    this.collection = database.getCollection(collectionName);
                }

                onConnect(newClient, snapshotsEnabled);
            } catch (MongoSocketException | MongoServerException e) {
                logger.warning("Could not connect to MongoDB", e);
            }
        }

        @Nonnull
        abstract Traverser<?> nextChunkTraverser();

        @Nullable
        abstract Object snapshot();

        abstract void restore(Object value);

        abstract boolean everCompletes();

        boolean supportsWatermarks() {
            return !everCompletes();
        }

        public abstract void restoreWatermark(Long value);

        public abstract Object watermark();
    }

    private final class BatchMongoReader extends MongoChunkedReader {
        private final FunctionEx<Document, I> mapItemFn;
        private final Aggregates aggregates;
        private Traverser<Document> delegate;
        private Object lastKey;

        private BatchMongoReader(
                String databaseName,
                String collectionName,
                FunctionEx<Document, I> mapItemFn,
                Aggregates aggregates) {
            super(databaseName, collectionName);
            this.mapItemFn = mapItemFn;
            this.aggregates = aggregates;
        }

        @Override
        void onConnect(MongoClient mongoClient, boolean supportsSnapshots) {
            List<Bson> aggregateList = new ArrayList<>(aggregates.nonNulls());
            if (supportsSnapshots && !hasSorts(aggregateList)) {
                aggregateList.add(sort(ascending("_id")).toBsonDocument());
            }
            if (supportsSnapshots && lastKey != null) {
                aggregateList.add(match(gt("_id", lastKey)).toBsonDocument());
            }
            if (canParallelize) {
                aggregateList.addAll(aggregates.indexAfterFilter(),
                        partitionAggregate(totalParallelism, processorIndex, false));
            }
            if (collection != null) {
                this.delegate = delegateForCollection(collection, aggregateList);
            } else if (database != null) {
                this.delegate = delegateForDb(database, aggregateList);
            } else {
                final MongoClient clientLocal = mongoClient;
                this.delegate = traverseIterable(mongoClient.listDatabaseNames())
                        .flatMap(name -> {
                            MongoDatabase db = clientLocal.getDatabase(name);
                            return delegateForDb(db, aggregateList);
                        });
            }
            checkNotNull(this.delegate, "unable to connect to Mongo");
        }

        private boolean hasSorts(List<Bson> aggregateList) {
            return aggregateList.stream().anyMatch(agg -> agg.toBsonDocument().get("$sort") != null);
        }

        private Traverser<Document> delegateForCollection(MongoCollection<Document> collection,
                                                          List<Bson> aggregateList) {
            return traverseIterable(collection.aggregate(aggregateList).batchSize(BATCH_SIZE));
        }

        private Traverser<Document> delegateForDb(MongoDatabase database, List<Bson> aggregateList) {
            MongoIterable<String> collectionsIterable = database.listCollectionNames();

            return traverseIterable(collectionsIterable)
                    .flatMap(colName -> delegateForCollection(database.getCollection(colName), aggregateList));
        }

        @Nonnull
        @Override
        public Traverser<I> nextChunkTraverser() {
            Traverser<Document> localDelegate = this.delegate;
            checkNotNull(localDelegate, "unable to connect to Mongo");
            return localDelegate
                    .map(item -> {
                        lastKey = item.get("_id");
                        return mapItemFn.apply(item);
                    });
        }

        @Override
        boolean everCompletes() {
            return true;
        }

        @Override
        public void restoreWatermark(Long value) {
            throw new UnsupportedOperationException("watermarks are only in streaming case");
        }

        @Override
        public Object watermark() {
            throw new UnsupportedOperationException("watermarks are only in streaming case");
        }

        @Nonnull
        @Override
        public Object snapshot() {
            return lastKey;
        }

        @Override
        public void restore(Object value) {
            lastKey = value;
        }

        @Override
        public void close() {
        }
    }

    private final class StreamMongoReader extends MongoChunkedReader {
        private final BiFunctionEx<ChangeStreamDocument<Document>, Long, I> mapFn;
        private final BsonTimestamp startTimestamp;
        private final Aggregates aggregates;
        private final EventTimeMapper<I> eventTimeMapper;
        private MongoCursor<ChangeStreamDocument<Document>> cursor;
        private BsonDocument resumeToken;

        private StreamMongoReader(
                String databaseName,
                String collectionName,
                BiFunctionEx<ChangeStreamDocument<Document>, Long, I> mapFn,
                BsonTimestamp startTimestamp,
                Aggregates aggregates,
                EventTimeMapper<I> eventTimeMapper
        ) {
            super(databaseName, collectionName);
            this.mapFn = mapFn;
            this.startTimestamp = startTimestamp;
            this.aggregates = aggregates;
            this.eventTimeMapper = eventTimeMapper;
        }

        @Override
        public void onConnect(MongoClient mongoClient, boolean snapshotsEnabled) {
            List<Bson> aggregateList = new ArrayList<>(aggregates.nonNulls());
            if (canParallelize) {
                aggregateList.addAll(aggregates.indexAfterFilter(),
                        partitionAggregate(totalParallelism, processorIndex, true));
            }
            ChangeStreamIterable<Document> changeStream;
            if (collection != null) {
                changeStream = collection.watch(aggregateList);
            } else if (database != null) {
                changeStream = database.watch(aggregateList);
            } else {
                changeStream = mongoClient.watch(aggregateList);
            }

            if (resumeToken != null) {
                changeStream.resumeAfter(resumeToken);
            } else if (startTimestamp != null) {
                changeStream.startAtOperationTime(startTimestamp);
            }
            cursor = changeStream.batchSize(BATCH_SIZE).fullDocument(UPDATE_LOOKUP).iterator();
        }

        @Override
        boolean everCompletes() {
            return false;
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        @Override
        public Traverser<?> nextChunkTraverser() {
            try {
                MongoCursor<ChangeStreamDocument<Document>> localCursor = this.cursor;
                checkNotNull(localCursor, "unable to connect to Mongo");
                return new CursorTraverser(localCursor)
                        .flatMap(input -> {
                            if (input instanceof EmptyItem) {
                                return eventTimeMapper.flatMapIdle();
                            }
                            ChangeStreamDocument<Document> doc = (ChangeStreamDocument<Document>) input;
                            resumeToken = doc.getResumeToken();
                            long eventTime = clusterTime(doc);
                            I item = mapFn.apply(doc, eventTime);
                            return eventTimeMapper.flatMapEvent(item, 0, eventTime);
                        });
            } catch (MongoException e) {
                throw new JetException("error while reading from mongodb", e);
            }
        }

        private long clusterTime(ChangeStreamDocument<Document> changeStreamDocument) {
            BsonDateTime time = changeStreamDocument.getWallTime();
            return time == null ? System.currentTimeMillis() : time.getValue();
        }

        @Nullable
        @Override
        public Object snapshot() {
            return resumeToken;
        }

        @Nonnull
        @Override
        public Object watermark() {
            return eventTimeMapper.getWatermark(0);
        }

        @Override
        public void restore(Object value) {
            if (value != null) {
                if (value instanceof BsonDocument) {
                    this.resumeToken = (BsonDocument) value;
                }
            }
        }

        @Override
        public void restoreWatermark(Long value) {
            eventTimeMapper.restoreWatermark(0, value);
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
