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
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClients;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;

import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static java.util.Arrays.asList;

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

    InsertProcessorSupplier(MongoTable table, WriteMode writeMode) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;
        this.paths = table.externalNames();
        this.types = table.fieldTypes();
        this.writeMode = writeMode;
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
                            .setDocumentIdentityFn(doc -> doc.get("_id"))
                            .setDocumentIdentityFieldName("_id")
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

            if (fieldName.equals("_id")) {
                if (value instanceof String) {
                    value = new ObjectId((String) value);
                } else if (value == null) {
                    continue;
                }
            } else if (value instanceof LocalDateTime) {
                Timestamp jdbcTimestamp = Timestamp.valueOf((LocalDateTime) value);
                value = new BsonDateTime(jdbcTimestamp.getTime());
            } else if (value instanceof OffsetDateTime) {
                OffsetDateTime v = (OffsetDateTime) value;
                ZonedDateTime atUtc = v.atZoneSameInstant(ZoneId.of("UTC"));
                Timestamp jdbcTimestamp = Timestamp.valueOf(atUtc.toLocalDateTime());
                value = new BsonDateTime(jdbcTimestamp.getTime());
            } else {
                // todo other coercions?
                value = types[i].convert(value);
            }
            doc = doc.append(fieldName, value);
        }

        return doc;
    }

}
