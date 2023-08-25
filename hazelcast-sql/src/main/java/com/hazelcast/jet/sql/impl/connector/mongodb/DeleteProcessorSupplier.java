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
import com.hazelcast.jet.mongodb.impl.UpdateMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.UPDATE_ALL_PREDICATE;
import static com.hazelcast.jet.sql.impl.connector.mongodb.DynamicallyReplacedPlaceholder.replacePlaceholdersInPredicate;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance
 * that will delete given item.
 */
public class DeleteProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final Serializable predicate;
    private final String[] externalNames;
    private final boolean hasInput;
    private transient SupplierEx<MongoClient> clientSupplier;
    private final String dataConnectionName;
    private final String idField;
    private ExpressionEvalContext evalContext;

    DeleteProcessorSupplier(MongoTable table, Serializable predicate, boolean hasInput) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.dataConnectionName = table.dataConnectionName;
        this.collectionName = table.collectionName;
        this.externalNames = table.externalNames();
        this.idField = table.primaryKeyExternalName();
        this.predicate = predicate;
        this.hasInput = hasInput;
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
            Processor processor;
            if (hasInput) {
                processor = new WriteMongoP<>(
                        new WriteMongoParams<>()
                                .setClientSupplier(clientSupplier)
                                .setDataConnectionRef(dataConnectionName)
                                .setDatabaseName(databaseName)
                                .setCollectionName(collectionName)
                                .setDocumentType(Object.class)
                                .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                                .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                                .setIntermediateMappingFn(this::rowToDoc)
                                .setWriteModelFn(this::delete)
                );

            } else {
                Document predicateWithReplacements = predicate == null
                        ? UPDATE_ALL_PREDICATE
                        : replacePlaceholdersInPredicate(predicate, externalNames, evalContext);
                processor = new UpdateMongoP<>(
                        new WriteMongoParams<Document>()
                                .setClientSupplier(clientSupplier)
                                .setDataConnectionRef(dataConnectionName)
                                .setDatabaseName(databaseName)
                                .setCollectionName(collectionName)
                                .setDocumentType(Document.class),
                        () -> new DeleteManyModel<>(predicateWithReplacements)
                );

            }
            processors[i] = processor;
        }

        return asList(processors);
    }

    private WriteModel<Object> delete(Object pkValue) {
        return new DeleteManyModel<>(Filters.eq(idField, pkValue));
    }

    private Object rowToDoc(JetSqlRow row) {
        assert row.getFieldCount() == 1;

        return row.getValues()[0];
    }

}
