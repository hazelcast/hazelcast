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
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance
 * that will delete given item.
 */
public class DeleteProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private transient SupplierEx<MongoClient> clientSupplier;
    private final String dataLinkName;
    private final String idField;

    DeleteProcessorSupplier(MongoTable table) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.dataLinkName = table.dataLinkName;
        this.collectionName = table.collectionName;
        this.idField = table.primaryKeyExternalName();
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        if (connectionString != null) {
            clientSupplier = () -> MongoClients.create(connectionString);
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Processor[] processors = new Processor[count];

        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<>()
                            .setClientSupplier(clientSupplier)
                            .setDataLinkRef(dataLinkName)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Object.class)
                            .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                            .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                            .setIntermediateMappingFn(this::rowToDoc)
                            .setWriteModelFn(this::delete)
                    );

            processors[i] = processor;
        }
        return asList(processors);
    }

    private WriteModel<Object> delete(Object pkValue) {
        return new DeleteOneModel<>(Filters.eq(idField, pkValue));
    }

    private Object rowToDoc(JetSqlRow row) {
        assert row.getFieldCount() == 1;

        return row.getValues()[0];
    }

}
