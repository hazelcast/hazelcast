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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.mongodb.impl.ReadMongoP;
import com.hazelcast.jet.mongodb.impl.WriteMongoP;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_COMMIT_RETRY_STRATEGY;
import static com.hazelcast.jet.mongodb.MongoDBSinkBuilder.DEFAULT_TRANSACTION_OPTION;
import static java.util.Arrays.asList;

/**
 * ProcessorSupplier that creates {@linkplain ReadMongoP} processors on each instance.
 */
public class InsertProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final String[] paths;
    private transient ExpressionEvalContext evalContext;

    InsertProcessorSupplier(MongoTable table) {
        this.connectionString = table.connectionString;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;
        paths = table.paths();
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Processor[] processors = new Processor[count];
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        for (int i = 0; i < count; i++) {
            Processor processor;
            processor  = new WriteMongoP<>(
                    () -> MongoClients.create(connectionString),
                    databaseName,
                    collectionName,
                    Document.class,
                    doc -> doc.getObjectId("_id"),
                    replaceOpts -> {
                    },
                    "_id",
                    DEFAULT_COMMIT_RETRY_STRATEGY,
                    serializationService.toBytes(DEFAULT_TRANSACTION_OPTION),
                    this::rowToDoc
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
            doc = doc.append(fieldName, values[i]);
        }

        return doc;
    }

}
