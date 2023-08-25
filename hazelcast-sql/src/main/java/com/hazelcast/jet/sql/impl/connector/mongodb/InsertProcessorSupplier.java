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
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.security.permission.ConnectorPermission;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonType;
import org.bson.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static com.hazelcast.security.permission.ActionConstants.ACTION_WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance
 * that will insert given item.
 */
public class InsertProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final String[] paths;
    private final WriteMode writeMode;
    private final QueryDataType[] types;
    private final BsonType[] externalTypes;
    private transient SupplierEx<MongoClient> clientSupplier;
    private final String dataConnectionName;
    private final String idField;

    InsertProcessorSupplier(MongoTable table, WriteMode writeMode) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.dataConnectionName = table.dataConnectionName;
        this.collectionName = table.collectionName;
        this.paths = table.externalNames();
        this.types = table.fieldTypes();
        this.externalTypes = table.externalTypes();
        this.writeMode = writeMode;
        this.idField = table.primaryKeyExternalName();
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        String connDetails = connectionString == null ? dataConnectionName : connectionString;
        return singletonList(ConnectorPermission.mongo(connDetails, databaseName, collectionName, ACTION_WRITE));
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

        final String idFieldName = idField;
        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<Document>()
                            .setClientSupplier(clientSupplier)
                            .setDataConnectionRef(dataConnectionName)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Document.class)
                            .setDocumentIdentityFn(doc -> doc.get(idFieldName))
                            .setDocumentIdentityFieldName(idFieldName)
                            .setCommitRetryStrategy(DEFAULT_COMMIT_RETRY_STRATEGY)
                            .setTransactionOptionsSup(() -> DEFAULT_TRANSACTION_OPTION)
                            .setIntermediateMappingFn(this::rowToDoc)
                            .setWriteMode(writeMode)
                    );

            processors[i] = processor;
        }
        return asList(processors);
    }

    private Document rowToDoc(JetSqlRow row) {
        Object[] values = row.getValues();
        Document doc = new Document();

        // assuming values is exactly the length of schema
        for (int i = 0; i < row.getFieldCount(); i++) {
            String fieldName = paths[i];
            Object value = values[i];

            if (fieldName.equals("_id") && value == null) {
                continue;
            }
            value = ConversionsToBson.convertToBson(value, types[i], externalTypes[i]);
            doc = doc.append(fieldName, value);
        }

        return doc;
    }

}
