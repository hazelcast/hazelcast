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
import com.hazelcast.jet.mongodb.impl.UpdateMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static com.hazelcast.jet.mongodb.impl.Mappers.defaultCodecRegistry;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.UPDATE_ALL_PREDICATE;
import static com.hazelcast.jet.sql.impl.connector.mongodb.DynamicallyReplacedPlaceholder.replacePlaceholdersInPredicate;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance,
 *  that will update given items.
 */
public class UpdateProcessorSupplier extends MongoProcessorSupplier implements DataSerializable {

    private String[] updatedFieldNames;
    private List<? extends Serializable> updates;
    private boolean afterScan;
    private ExpressionEvalContext evalContext;
    private String pkExternalName;
    private Serializable predicate;

    @SuppressWarnings("unused")
    public UpdateProcessorSupplier() {
    }

    UpdateProcessorSupplier(MongoTable table,
                            @Nonnull String[] updatedFieldNames,
                            List<? extends Serializable> updates,
                            Serializable predicate,
                            boolean hasInput) {
        super(table);
        // update-specific
        this.updatedFieldNames = updatedFieldNames;
        this.updates = updates;
        this.pkExternalName = table.primaryKeyExternalName();
        this.predicate = predicate;

        this.afterScan = hasInput;
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

        if (!afterScan) {
            Document predicateWithReplacements = predicate == null
                    ? UPDATE_ALL_PREDICATE
                    : replacePlaceholdersInPredicate(predicate, externalNames, evalContext);
            for (int i = 0; i < count; i++) {
                Processor processor = new UpdateMongoP<>(
                        new WriteMongoParams<Document>()
                                .setClientSupplier(clientSupplier)
                                .setDataConnectionRef(dataConnectionName)
                                .setDatabaseName(databaseName)
                                .setCollectionName(collectionName)
                                .setDocumentType(Document.class)
                                .setCheckExistenceOnEachConnect(checkExistenceOnEachConnect),
                        writeModelNoScan(predicateWithReplacements)
                );

                processors[i] = processor;
            }
            return asList(processors);
        }

        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<Document>()
                            .setClientSupplier(clientSupplier)
                            .setDataConnectionRef(dataConnectionName)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Document.class)
                            .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                            .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                            .setIntermediateMappingFn(this::rowToUpdateDoc)
                            .setWriteModelFn(this::writeModelAfterScan)
                            .setCheckExistenceOnEachConnect(checkExistenceOnEachConnect)
            );

            processors[i] = processor;
        }
        return asList(processors);
    }

    @SuppressWarnings("unchecked")
    private SupplierEx<WriteModel<Document>> writeModelNoScan(Document predicate) {
        return () -> {
            Document values = valuesToUpdateDoc(externalNames);
            List<Bson> pipeline = values.get("update", List.class);
            WriteModel<Document> doc = new UpdateManyModel<>(predicate, pipeline);
            return doc;
        };
    }

    @SuppressWarnings("unchecked")
    private WriteModel<Document> writeModelAfterScan(Document update) {
        List<Bson> updates = requireNonNull(update.get("update", List.class), "updateList");
        Bson filter = requireNonNull(update.get("filter", Bson.class));
        return new UpdateManyModel<>(filter, updates);
    }

    /**
     * Row parameter contains values for PK fields, values of input before update, values of input after update.
     */
    private Document rowToUpdateDoc(JetSqlRow row) {
        Object[] values = row.getValues();
        return valuesToUpdateDoc(values);
    }

    /**
     * Row parameter contains values for PK fields, values of input before update, values of input after update, but
     * only when the update is performed after a scan.
     *
     * <p>If it's the scan-less mode, input references must be made using Mongo's {@code $reference} syntax,
     * because there is no "previous value" in the row.</p>
     */
    private Document valuesToUpdateDoc(Object[] values) {
        Object pkValue = values[0];

        List<Bson> updateToPerform = new ArrayList<>();
        for (int i = 0; i < updatedFieldNames.length; i++) {
            String fieldName = updatedFieldNames[i];
            Object updateExpr = updates.get(i);
            if (updateExpr instanceof Bson) {
                Document document = Document.parse(((Bson) updateExpr)
                        .toBsonDocument(Document.class, defaultCodecRegistry()).toJson());
                PlaceholderReplacer.replacePlaceholders(document, evalContext, values, externalNames, afterScan);
                updateExpr = document;
                updateToPerform.add(Aggregates.set(new Field<>(fieldName, updateExpr)));
            } else if (updateExpr instanceof String) {
                String expr = (String) updateExpr;
                Object withReplacements = PlaceholderReplacer.replace(expr, evalContext, values, externalNames, false,
                        afterScan);
                updateToPerform.add(Aggregates.set(new Field<>(fieldName, withReplacements)));
            } else {
                updateToPerform.add(Aggregates.set(new Field<>(fieldName, updateExpr)));
            }
        }

        Bson filter = Filters.eq(pkExternalName, pkValue);
        return new Document("filter", filter)
                .append("update", updateToPerform);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(connectionString);
        out.writeString(databaseName);
        out.writeString(collectionName);
        out.writeStringArray(updatedFieldNames);
        out.writeObject(updates);
        out.writeString(dataConnectionName);
        out.writeStringArray(externalNames);
        out.writeBoolean(afterScan);
        out.writeString(pkExternalName);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        connectionString = in.readString();
        databaseName = in.readString();
        collectionName = in.readString();
        updatedFieldNames = in.readStringArray();
        updates = in.readObject();
        dataConnectionName = in.readString();
        externalNames = in.readStringArray();
        afterScan = in.readBoolean();
        pkExternalName = in.readString();
        predicate = in.readObject();
    }
}
