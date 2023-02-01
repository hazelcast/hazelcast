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
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.mongodb.ReadMongoP;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * ProcessorSupplier that creates {@linkplain ReadMongoP} processors on each instance.
 */
public class SelectProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final boolean stream;
    private final FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private final boolean needTwoSteps;
    private final String predicate;
    private final List<String> projection;
    private final boolean containsMappingForId;
    private final Long startAt;
    private transient ExpressionEvalContext evalContext;

    SelectProcessorSupplier(MongoTable table, String predicate, List<String> projection, Long startAt, boolean stream,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
                            boolean needTwoSteps) {
        this.predicate = predicate;
        this.projection = projection;
        this.containsMappingForId = table.hasMappingForId();
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;
        this.startAt = startAt;
        this.stream = stream;
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.needTwoSteps = needTwoSteps;
        checkArgument(projection != null && !projection.isEmpty(), "projection cannot be empty");
    }

    SelectProcessorSupplier(MongoTable table, String predicate, List<String> projection, Long startAt,
                            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
                            boolean needTwoSteps) {
        this(table, predicate, projection, startAt, true, eventTimePolicyProvider, needTwoSteps);
    }

    SelectProcessorSupplier(MongoTable table, String predicate, List<String> projection, boolean needTwoSteps) {
        this(table, predicate, projection, null, false, null, needTwoSteps);
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        ArrayList<Bson> aggregates = new ArrayList<>();

        if (this.predicate != null) {
            Document filterDoc = Document.parse(this.predicate);
            Bson filterWithParams = ParameterReplacer.replacePlaceholders(filterDoc, evalContext);
            aggregates.add(match(filterWithParams.toBsonDocument()));
        }
        Bson proj = include(this.projection);
        if (!projection.contains("_id") && !stream) {
            aggregates.add(project(fields(excludeId(), proj)));
        } else {
            aggregates.add(project(proj));
        }

        Processor[] processors = new Processor[count];

        EventTimePolicy<JetSqlRow> eventTimePolicy = eventTimePolicyProvider == null
                ? EventTimePolicy.noEventTime()
                : eventTimePolicyProvider.apply(evalContext);
        for (int i = 0; i < count; i++) {
            Processor processor;
            if (!stream) {
                processor  = new ReadMongoP<>(
                        () -> MongoClients.create(connectionString),
                        aggregates,
                        databaseName,
                        collectionName,
                        this::convertDocToRow
                );
            } else {
                processor  = new ReadMongoP<>(
                        () -> MongoClients.create(connectionString),
                        startAt,
                        eventTimePolicy,
                        aggregates,
                        databaseName,
                        collectionName,
                        this::convertStreamDocToRow
                );
            }

            processors[i] = processor;
        }
        return asList(processors);
    }

    private JetSqlRow convertDocToRow(Document doc) {
        Object[] row = new Object[doc.size()];

        int i = 0;
        for (Object value : doc.values()) {
            row[i++] = value;
        }

        return new JetSqlRow(evalContext.getSerializationService(), row);
    }

    private JetSqlRow convertStreamDocToRow(ChangeStreamDocument<Document> changeStreamDocument) {
        Document doc = changeStreamDocument.getFullDocument();
        requireNonNull(doc, "Document is empty");
        Object[] row = new Object[projection.size()];

        for (Entry<String, Object> entry : doc.entrySet()) {
            int index = indexInProjection(entry.getKey());
            if (index == -1) {
                continue;
            }
            row[index] = entry.getValue();
        }
        addIfInProjection(changeStreamDocument.getOperationType().getValue(), "operationType", row);
        addIfInProjection(changeStreamDocument.getResumeToken().toString(), "resumeToken", row);

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
