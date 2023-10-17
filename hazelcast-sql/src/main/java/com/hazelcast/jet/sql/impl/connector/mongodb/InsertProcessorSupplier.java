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
import com.hazelcast.jet.mongodb.WriteMode;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoParams;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClients;
import org.bson.BsonType;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance
 * that will insert given item.
 */
public class InsertProcessorSupplier extends MongoProcessorSupplier implements DataSerializable {

    private WriteMode writeMode;
    private QueryDataType[] types;
    private BsonType[] externalTypes;
    private String idField;

    @SuppressWarnings("unused")
    public InsertProcessorSupplier() {
    }

    InsertProcessorSupplier(MongoTable table, WriteMode writeMode) {
        super(table);
        this.types = table.fieldTypes();
        this.externalTypes = table.externalTypes();
        this.writeMode = writeMode;
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
                            .setCheckExistenceOnEachConnect(checkExistenceOnEachConnect)
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
            String fieldName = externalNames[i];
            Object value = values[i];

            if (fieldName.equals("_id") && value == null) {
                continue;
            }
            value = ConversionsToBson.convertToBson(value, types[i], externalTypes[i]);
            doc = doc.append(fieldName, value);
        }

        return doc;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(connectionString);
        out.writeString(databaseName);
        out.writeString(collectionName);
        out.writeStringArray(externalNames);
        out.writeString(writeMode == null ? null : writeMode.name());
        out.writeObject(types);
        out.writeInt(externalTypes == null ? 0 : externalTypes.length);
        for (BsonType externalType : externalTypes) {
            out.writeInt(externalType.getValue());
        }
        out.writeString(dataConnectionName);
        out.writeString(idField);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        connectionString = in.readString();
        databaseName = in.readString();
        collectionName = in.readString();
        externalNames = in.readStringArray();
        String writeModeName = in.readString();
        writeMode = writeModeName == null ? null : WriteMode.valueOf(writeModeName);
        types = in.readObject();
        int howManyExtTypes = in.readInt();
        var extTypes = new BsonType[howManyExtTypes];
        for (int i = 0; i < howManyExtTypes; i++) {
            extTypes[i] = BsonType.findByValue(in.readInt());
        }
        externalTypes = extTypes;
        dataConnectionName = in.readString();
        idField = in.readString();
    }
}
