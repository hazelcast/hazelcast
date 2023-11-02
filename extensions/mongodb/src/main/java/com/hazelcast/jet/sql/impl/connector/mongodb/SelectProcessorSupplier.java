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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.mongodb.impl.ReadMongoP;
import com.hazelcast.jet.mongodb.impl.ReadMongoParams;
import com.hazelcast.jet.mongodb.impl.ReadMongoParams.Aggregates;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.hazelcast.jet.mongodb.impl.Mappers.bsonToDocument;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonDateTimeToLocalDateTime;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonTimestampToLocalDateTime;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static java.time.ZoneId.systemDefault;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * ProcessorSupplier that creates {@linkplain com.hazelcast.jet.mongodb.impl.ReadMongoP} processors on each instance.
 */
public class SelectProcessorSupplier extends MongoProcessorSupplier implements DataSerializable {

    private FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private Document predicate;
    private List<ProjectionData> projection;

    private Long startAt;
    private boolean stream;
    private boolean forceMongoParallelismOne;

    private transient ExpressionEvalContext evalContext;

    @SuppressWarnings("unused")
    public SelectProcessorSupplier() {
    }

    SelectProcessorSupplier(MongoTable table, Document predicate,
                            List<ProjectionData> projection,
                            BsonTimestamp startAt,
                            boolean stream,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        super(table);
        checkArgument(projection != null && !projection.isEmpty(), "projection cannot be empty");
        this.predicate = predicate;
        this.projection = projection;
        this.startAt = startAt == null ? null : startAt.getValue();
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.stream = stream;
        this.forceMongoParallelismOne = table.isforceReadTotalParallelismOne();
    }

    SelectProcessorSupplier(MongoTable table, Document predicate,
                            List<ProjectionData> projection, BsonTimestamp startAt,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        this(table, predicate, projection, startAt, true, eventTimePolicyProvider);
    }

    SelectProcessorSupplier(MongoTable table, Document predicate,
                            List<ProjectionData> projection) {
        this(table, predicate, projection, null, false, null);
    }

    @Override
    public void init(@Nonnull Context context) {
        if (connectionString != null) {
            clientSupplier = () -> MongoClients.create(connectionString);
        }
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Aggregates aggregates = new Aggregates();

        if (this.predicate != null) {
            Bson filterWithParams = PlaceholderReplacer.replacePlaceholders(predicate, evalContext, (Object[]) null,
                    externalNames, false);
            aggregates.setFilter(bsonToDocument(match(filterWithParams.toBsonDocument())));
        }
        Bson proj = fields(projection.stream().map(p -> p.projectionExpr).collect(toList()));
        List<String> projectedNames = projection.stream().map(p -> p.externalName).collect(toList());
        if (!projectedNames.contains("_id") && !stream) {
            aggregates.setProjection(bsonToDocument(project(fields(excludeId(), proj))));
        } else {
            aggregates.setProjection(bsonToDocument(project(proj)));
        }

        List<Processor> processors = new ArrayList<>();

        EventTimePolicy<JetSqlRow> eventTimePolicy = eventTimePolicyProvider == null
                ? EventTimePolicy.noEventTime()
                : eventTimePolicyProvider.apply(evalContext);
        SupplierEx<? extends MongoClient> clientSupplierEx = clientSupplier;
        for (int i = 0; i < count; i++) {
            Processor processor = new ReadMongoP<>(
                    new ReadMongoParams<JetSqlRow>(stream)
                            .setClientSupplier(clientSupplierEx)
                            .setDataConnectionRef(dataConnectionName)
                            .setAggregates(aggregates)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setMapItemFn(this::convertDocToRow)
                            .setMapStreamFn(this::convertStreamDocToRow)
                            .setStartAtTimestamp(startAt == null ? null : new BsonTimestamp(startAt))
                            .setEventTimePolicy(eventTimePolicy)
                            .setNonDistributed(forceMongoParallelismOne)
                            .setCheckExistenceOnEachConnect(checkExistenceOnEachConnect)
            );

            processors.add(processor);
        }
        return processors;
    }

    private JetSqlRow convertDocToRow(Document doc) {
        Object[] row = new Object[projection.size()];

        for (ProjectionData entry : projection) {
            Object fromDoc = doc.get(entry.externalName);
            int index = entry.index;
            if (entry.type != null) {
                row[index] = ConversionsFromBson.convertFromBson(fromDoc, entry.type);
            } else {
                row[index] = fromDoc;
            }
        }

        return new JetSqlRow(evalContext.getSerializationService(), row);
    }

    private JetSqlRow convertStreamDocToRow(ChangeStreamDocument<Document> changeStreamDocument, Long ts) {
        Document doc = changeStreamDocument.getFullDocument();
        requireNonNull(doc, "Document is empty");
        Object[] row = new Object[projection.size()];

        for (ProjectionData entry : projection) {
            Object fromDoc = doc.get(entry.externalName.replaceFirst("fullDocument.", ""));
            int index = entry.index;
            if (entry.type != null) {
                row[index] = ConversionsFromBson.convertFromBson(fromDoc, entry.type);
            } else {
                row[index] = fromDoc;
            }
        }
        addIfInProjection(changeStreamDocument.getOperationTypeString(), "operationType", row);
        addIfInProjection(changeStreamDocument.getResumeToken().toString(), "resumeToken", row);
        addIfInProjection(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), systemDefault()), "ts", row);
        addIfInProjection(bsonDateTimeToLocalDateTime(changeStreamDocument.getWallTime()), "wallTime", row);
        addIfInProjection(bsonTimestampToLocalDateTime(changeStreamDocument.getClusterTime()), "clusterTime", row);

        return new JetSqlRow(evalContext.getSerializationService(), row);
    }

    private void addIfInProjection(Object value, String field, Object[] row) {
        int index = indexInProjection(field);
        if (index == -1) {
            return;
        }
        row[index] = value;
    }

    private int indexInProjection(String columnName) {
        return projection.stream().filter(p -> p.externalName.equals(columnName))
                         .map(p -> p.index)
                         .findAny().orElse(-1);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(databaseName);
        out.writeString(collectionName);
        out.writeBoolean(stream);
        out.writeObject(eventTimePolicyProvider);
        out.writeObject(predicate);
        out.writeObject(projection);
        out.writeStringArray(externalNames);
        out.writeLong(startAt == null ? -1 : startAt);
        out.writeString(connectionString);
        out.writeString(dataConnectionName);
        out.writeBoolean(forceMongoParallelismOne);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        databaseName = in.readString();
        collectionName = in.readString();
        stream = in.readBoolean();
        eventTimePolicyProvider = in.readObject();
        predicate = in.readObject();
        projection = in.readObject();
        externalNames = in.readStringArray();
        long startAtDirect = in.readLong();
        startAt = startAtDirect == -1 ? null : startAtDirect;
        connectionString = in.readString();
        dataConnectionName = in.readString();
        forceMongoParallelismOne = in.readBoolean();
    }
}
