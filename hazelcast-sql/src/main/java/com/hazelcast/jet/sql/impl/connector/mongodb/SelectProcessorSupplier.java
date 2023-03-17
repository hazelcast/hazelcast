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
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.mongodb.impl.ReadMongoP;
import com.hazelcast.jet.mongodb.impl.ReadMongoParams;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonDateTimeToLocalDateTime;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonTimestampToLocalDateTime;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Objects.requireNonNull;

/**
 * ProcessorSupplier that creates {@linkplain com.hazelcast.jet.mongodb.impl.ReadMongoP} processors on each instance.
 */
public class SelectProcessorSupplier implements ProcessorSupplier {

    private transient SupplierEx<? extends MongoClient> clientSupplier;
    private final String databaseName;
    private final String collectionName;
    private final boolean stream;
    private final FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private final Document predicate;
    private final List<String> projection;
    private final Long startAt;
    private final String connectionString;
    private final String dataLinkName;
    private transient ExpressionEvalContext evalContext;
    private final QueryDataType[] types;

    SelectProcessorSupplier(MongoTable table, Document predicate, List<String> projection, BsonTimestamp startAt, boolean stream,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        checkArgument(projection != null && !projection.isEmpty(), "projection cannot be empty");

        this.predicate = predicate;
        this.projection = projection;
        this.connectionString = table.connectionString;
        this.dataLinkName = table.dataLinkName;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;
        this.startAt = startAt == null ? null : startAt.getValue();
        this.stream = stream;
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.types = table.resolveColumnTypes(projection);
    }


    SelectProcessorSupplier(MongoTable table, Document predicate, List<String> projection, BsonTimestamp startAt,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider) {
        this(table, predicate, projection, startAt, true, eventTimePolicyProvider);
    }

    SelectProcessorSupplier(MongoTable table, Document predicate, List<String> projection) {
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
        ArrayList<Bson> aggregates = new ArrayList<>();

        if (this.predicate != null) {
            Bson filterWithParams = ParameterReplacer.replacePlaceholders(predicate, evalContext);
            aggregates.add(match(filterWithParams.toBsonDocument()));
        }
        Bson proj = include(this.projection);
        if (!projection.contains("_id") && !stream) {
            aggregates.add(project(fields(excludeId(), proj)));
        } else {
            aggregates.add(project(proj));
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
                            .setDataLinkRef(dataLinkName)
                            .setAggregates(aggregates)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setMapItemFn(this::convertDocToRow)
                            .setMapStreamFn(this::convertStreamDocToRow)
                            .setStartAtTimestamp(startAt == null ? null : new BsonTimestamp(startAt))
                            .setEventTimePolicy(eventTimePolicy)
            );

            processors.add(processor);
        }
        return processors;
    }

    private JetSqlRow convertDocToRow(Document doc) {
        Object[] row = new Object[projection.size()];

        for (Map.Entry<String, Object> value : doc.entrySet()) {
            int index = indexInProjection(value.getKey());
            if (index != -1) {
                row[index] = ConversionsFromBson.convertFromBson(value.getValue(), types[index]);
            }
        }

        return new JetSqlRow(evalContext.getSerializationService(), row);
    }

    private JetSqlRow convertStreamDocToRow(ChangeStreamDocument<Document> changeStreamDocument, Long ts) {
        Document doc = changeStreamDocument.getFullDocument();
        requireNonNull(doc, "Document is empty");
        Object[] row = new Object[projection.size()];

        for (Entry<String, Object> entry : doc.entrySet()) {
            int index = indexInProjection(entry.getKey());
            if (index == -1) {
                continue;
            }
            row[index] = ConversionsFromBson.convertFromBson(entry.getValue(), types[index]);
        }
        addIfInProjection(changeStreamDocument.getOperationType().getValue(), "operationType", row);
        addIfInProjection(changeStreamDocument.getResumeToken().toString(), "resumeToken", row);
        addIfInProjection(ts, "ts", row);
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
        int index = projection.indexOf(columnName);
        if (index == -1) {
            index = projection.indexOf("fullDocument." + columnName);
        }
        return index;
    }
}
