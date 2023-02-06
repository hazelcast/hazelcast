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
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.Table;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance,
 *  that will update given items.
 */
public class UpdateProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final List<String> fieldNames;
    private final List<Expression<?>> updates;
    private final List<String> pkFields;
    private final ExpressionToMongoVisitor visitor;

    UpdateProcessorSupplier(MongoTable table, List<String> fieldNames, List<Expression<?>> updates) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;

        // update-specific
        this.fieldNames = fieldNames;
        this.updates = updates;
        this.pkFields = Collections.singletonList("_id");
        this.visitor = new ExpressionToMongoVisitor(table, null, false);
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        ExpressionEvalContext evalContext = ExpressionEvalContext.from(context);
        visitor.setContext(evalContext);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Processor[] processors = new Processor[count];

        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<Document>()
                            .setClientSupplier(() -> MongoClients.create(connectionString))
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Document.class)
                            .setDocumentIdentityFn(doc -> doc.getObjectId("_id"))
                            .setDocumentIdentityFieldName("_id")
                            .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                            .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                            .setIntermediateMappingFn(this::rowToUpdateDoc)
                            .setWriteMode(WriteMode.UPDATE_ONLY)
                            .setWriteModelFn(this::write)
            );

            processors[i] = processor;
        }
        return asList(processors);
    }

    private WriteModel<Document> write(Document row) {
        List<Bson> updateToPerform = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            Expression<?> updateExpr = updates.get(i);
            Object value = updateExpr.accept(visitor);

            updateToPerform.add(Updates.set(fieldName, value));
        }

        List<Bson> all = new ArrayList<>();
        for (String field : pkFields) {
            all.add(Filters.eq(field, row.get(field)));
        }
        Bson filter = Filters.and(all);
        return new UpdateOneModel<>(filter, updateToPerform);
    }

    /**
     * Row parameter contains values for PK fields.
     *
     * In Mongo we - for now - assume only default {@code _id} field, as it's
     * in the {@linkplain MongoSqlConnectorBase#getPrimaryKey(Table)}.
     */
    private Document rowToUpdateDoc(JetSqlRow row) {
        Object[] values = row.getValues();

        Document doc = new Document();
        int index = 0;
        for (String pkField : pkFields)  {
            Object value = values[index++];
            if (value instanceof String) {
                value = new ObjectId((String) value);
            }
            doc.append(pkField, value);
        }

        return doc;
    }

}
