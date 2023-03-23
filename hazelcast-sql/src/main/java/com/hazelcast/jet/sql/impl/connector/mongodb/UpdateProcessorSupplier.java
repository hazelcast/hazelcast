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
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonType;
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

/**
 * ProcessorSupplier that creates {@linkplain WriteMongoP} processors on each instance,
 *  that will update given items.
 */
public class UpdateProcessorSupplier implements ProcessorSupplier {

    private final String connectionString;
    private final String databaseName;
    private final String collectionName;
    private final List<String> fieldNames;
    private final List<? extends Serializable> updates;
    private final String dataLinkName;
    private final QueryDataType pkType;
    private final BsonType pkExternalType;
    private ExpressionEvalContext evalContext;
    private transient SupplierEx<MongoClient> clientSupplier;
    private final String pkExternalName;

    UpdateProcessorSupplier(MongoTable table, List<String> fieldNames, List<? extends Serializable> updates) {
        this.connectionString = table.connectionString;
        this.dataLinkName = table.dataLinkName;
        this.databaseName = table.databaseName;
        this.collectionName = table.collectionName;

        this.pkType = table.pkType();
        this.pkExternalType = table.pkExternalType();

        // update-specific
        this.fieldNames = fieldNames;
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

        final String idFieldName = pkExternalName;
        for (int i = 0; i < count; i++) {
            Processor processor = new WriteMongoP<>(
                    new WriteMongoParams<Document>()
                            .setClientSupplier(clientSupplier)
                            .setDataLinkRef(dataLinkName)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setDocumentType(Document.class)
                            .setDocumentIdentityFn(doc -> doc.get(idFieldName))
                            .setDocumentIdentityFieldName(idFieldName)
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
            Object updateExpr = updates.get(i);
            if (updateExpr instanceof Bson) {
                Document document = Document.parse(((Bson) updateExpr)
                                            .toBsonDocument(Document.class, defaultCodecRegistry()).toJson());
                ParameterReplacer.replacePlaceholders(document, evalContext);
                updateExpr = document;
            } else if (updateExpr instanceof DynamicParameter) {
                updateExpr = evalContext.getArgument(((DynamicParameter) updateExpr).getIndex());
            }

            updateToPerform.add(Updates.set(fieldName, updateExpr));
        }

        Bson filter = Filters.eq(pkExternalName, row.get(pkExternalName));
        return new UpdateOneModel<>(filter, updateToPerform);
    }

    /**
     * Row parameter contains values for PK fields.
     */
    private Document rowToUpdateDoc(JetSqlRow row) {
        Object[] values = row.getValues();

        Document doc = new Document();
        Object value = values[0];
        doc.append(pkExternalName, ConversionsToBson.convertToBson(value, pkType, pkExternalType));

        return doc;
    }

}
