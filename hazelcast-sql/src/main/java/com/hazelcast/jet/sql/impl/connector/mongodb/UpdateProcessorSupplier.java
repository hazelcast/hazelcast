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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance,
 *  that will update given items.
 */
public class UpdateProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final List<String> updatedFieldNames;
    private final List<? extends Serializable> updates;
    private final String dataLinkName;
    private ExpressionEvalContext evalContext;
    private transient SupplierEx<MongoClient> clientSupplier;
    private final String pkExternalName;

    UpdateProcessorSupplier(MongoTable table, List<String> updatedFieldNames, List<? extends Serializable> updates) {
        this.connectionString = table.connectionString;
        this.dataLinkName = table.dataLinkName;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;

        // update-specific
        this.updatedFieldNames = updatedFieldNames;
        this.updates = updates;
        this.pkExternalName = table.primaryKeyExternalName();
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        if (connectionString != null) {
            clientSupplier = () -> MongoClients.create(connectionString);
        }
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Processor[] processors = new Processor[count];

        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<Document>()
                            .setClientSupplier(clientSupplier)
                            .setDataLinkRef(dataLinkName)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Document.class)
                            .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                            .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                            .setIntermediateMappingFn(this::rowToUpdateDoc)
                            .setWriteModelFn(this::write)
            );

            processors[i] = processor;
        }
        return asList(processors);
    }

    @SuppressWarnings("unchecked")
    private WriteModel<Document> write(Document update) {
        List<? extends Bson> updates = requireNonNull(update.get("update", List.class), "updateList");
        Bson filter = requireNonNull(update.get("filter", Bson.class));
        return new UpdateOneModel<>(filter, updates);
    }

    /**
     * Row parameter contains values for PK fields, values of input before update, values of input after update.
     */
    private Document rowToUpdateDoc(JetSqlRow row) {
        Object[] values = row.getValues();

        Object pkValue = values[0];

        List<Bson> updateToPerform = new ArrayList<>();
        for (int i = 0; i < updatedFieldNames.size(); i++) {
            String fieldName = updatedFieldNames.get(i);
            Object updateExpr = updates.get(i);
            if (updateExpr instanceof Bson) {
                Document document = Document.parse(((Bson) updateExpr)
                        .toBsonDocument(Document.class, defaultCodecRegistry()).toJson());
                PlaceholderReplacer.replacePlaceholders(document, evalContext, row);
                updateExpr = document;
                updateToPerform.add(Aggregates.set(new Field<>(fieldName, updateExpr)));
            } else if (updateExpr instanceof String) {
                DynamicParameter parameter = DynamicParameter.matches(updateExpr);
                if (parameter != null) {
                    updateExpr = evalContext.getArgument(parameter.getIndex());
                    updateToPerform.add(Updates.set(fieldName, updateExpr));
                } else {
                    InputRef ref = InputRef.match(updateExpr);
                    if (ref != null) {
                        int index = ref.getInputIndex();
                        updateToPerform.add(Aggregates.set(new Field<>(fieldName, row.get(index))));
                    } else {
                        updateToPerform.add(Aggregates.set(new Field<>(fieldName, updateExpr)));
                    }
                }
            } else {
                updateToPerform.add(Aggregates.set(new Field<>(fieldName, updateExpr)));
            }


        }

        Bson filter = Filters.eq(pkExternalName, pkValue);
        return new Document("filter", filter).append("update", updateToPerform);
    }

}
