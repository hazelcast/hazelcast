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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.mongodb.ReadMongoP;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain ReadMongoP} processors on each instance.
 */
public class SelectProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final String predicate;
    private final List<String> projection;
    private final boolean containsMappingForId;
    private transient ExpressionEvalContext evalContext;

    SelectProcessorSupplier(MongoTable table, String predicate, List<String> projection) {
        this.predicate = predicate;
        this.projection = projection;
        this.containsMappingForId = table.hasMappingForId();
        connectionString = table.connectionString;
        databaseName = table.databaseName;
        collectionName = table.collectionName;
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
        if (this.projection != null && !this.projection.isEmpty()) {
            Bson proj = include(this.projection);
            if (!projection.contains("_id")) {
                aggregates.add(project(fields(excludeId(), proj)));
            } else {
                aggregates.add(project(proj));
            }
        } else if (!containsMappingForId) {
            aggregates.add(project(excludeId()));
        }

        Processor[] processors = new Processor[count];
        for (int i = 0; i < count; i++) {
            Processor processor = new ReadMongoP<>(
                    () -> MongoClients.create(connectionString),
                    aggregates,
                    databaseName,
                    collectionName,
                    this::convertToRow
            );
            processors[i] = processor;
        }
        return asList(processors);
    }

    private JetSqlRow convertToRow(Document doc) {
        Object[] row = new Object[doc.size()];

        int i = 0;
        for (Object value : doc.values()) {
            row[i++] = value;
        }

        return new JetSqlRow(evalContext.getSerializationService(), row);
    }
}
