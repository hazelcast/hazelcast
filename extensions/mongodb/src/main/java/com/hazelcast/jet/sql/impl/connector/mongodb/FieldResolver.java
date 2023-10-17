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

import com.google.common.annotations.VisibleForTesting;
import com.hazelcast.jet.mongodb.ResourceChecks;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonType;
import org.bson.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import static com.hazelcast.jet.mongodb.impl.MongoUtilities.checkDatabaseAndCollectionExists;
import static com.hazelcast.jet.sql.impl.connector.mongodb.BsonTypes.getBsonType;
import static com.hazelcast.jet.sql.impl.connector.mongodb.BsonTypes.resolveTypeFromJava;
import static com.hazelcast.jet.sql.impl.connector.mongodb.Options.CONNECTION_STRING_OPTION;
import static com.hazelcast.jet.sql.impl.connector.mongodb.Options.readExistenceChecksFlag;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;

class FieldResolver {

    private final NodeEngine nodeEngine;

    FieldResolver(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    /**
     * Resolves fields based on the options passed to the connector and schema provided by user (if any).
     *
     * @param externalName       external name of the mapping, namely collection name
     * @param dataConnectionName name of data connection used to create this mapping
     * @param options            options provided for the connector
     * @param userFields         user-provided field list
     * @return resolved fields - all fields from collection or user provided fields with resolved types
     * @throws IllegalArgumentException if given column type won't be resolved or field from user mapping won't exist
     *                                  in the collection
     */
    List<MappingField> resolveFields(
            @Nonnull String[] externalName,
            @Nullable String dataConnectionName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields,
            boolean stream
    ) {
        Predicate<MappingField> pkColumnName = Options.getPkColumnChecker(options, stream);
        Map<String, DocumentField> dbFields = readFields(externalName, dataConnectionName, options, stream);

        List<MappingField> resolvedFields = new ArrayList<>();
        if (userFields.isEmpty()) {
            for (DocumentField documentField : dbFields.values()) {
                MappingField mappingField = new MappingField(
                        documentField.columnName,
                        resolveType(documentField.columnType),
                        documentField.columnName,
                        documentField.columnType.name()
                );
                mappingField.setPrimaryKey(pkColumnName.test(mappingField));
                resolvedFields.add(mappingField);
            }
        } else {
            for (MappingField f : userFields) {
                String prefixIfStream = stream ? "fullDocument." : "";
                String nameInMongo = f.externalName() == null ? prefixIfStream + f.name() : f.externalName();

                DocumentField documentField = getField(dbFields, f, stream);
                if (documentField == null) {
                    throw new IllegalArgumentException("Could not resolve field with name " + nameInMongo);
                }
                MappingField mappingField = new MappingField(f.name(), f.type(), documentField.columnName,
                        documentField.columnType.name());
                mappingField.setPrimaryKey(pkColumnName.test(mappingField));
                validateType(f, documentField);
                resolvedFields.add(mappingField);
            }
        }
        return resolvedFields;
    }

    private DocumentField getField(Map<String, DocumentField> dbFields, MappingField f, boolean stream) {
        String externalName = f.externalName() == null ? f.name() : f.externalName();
        if (stream) {
            String withPrefix = "fullDocument." + externalName;
            if (dbFields.containsKey(withPrefix)) {
                return dbFields.get(withPrefix);
            } else {
                return dbFields.get(externalName);
            }
        } else {
            return dbFields.get(externalName);
        }
    }

    boolean isId(String nameInMongo, boolean stream) {
        if (stream) {
            return "fullDocument._id".equalsIgnoreCase(nameInMongo);
        }
        return "_id".equalsIgnoreCase(nameInMongo);
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private QueryDataType resolveType(BsonType columnType) {
        switch (columnType) {
            case INT32: return QueryDataType.INT;
            case INT64: return QueryDataType.BIGINT;
            case DOUBLE: return QueryDataType.DOUBLE;
            case BOOLEAN: return QueryDataType.BOOLEAN;
            case TIMESTAMP:
            case DATE_TIME:
                return QueryDataType.TIMESTAMP;
            case STRING:
            case JAVASCRIPT:
            case JAVASCRIPT_WITH_SCOPE:
                return VARCHAR;
            case DECIMAL128: return QueryDataType.DECIMAL;
            case OBJECT_ID:
            case BINARY:
            case MIN_KEY:
            case ARRAY:
            case REGULAR_EXPRESSION:
            case MAX_KEY: return QueryDataType.OBJECT;
            case DOCUMENT: return QueryDataType.JSON;
            default:  throw QueryException.error("BSON type " + columnType + " is not yet supported");
        }
    }

    private void validateType(MappingField field, DocumentField documentField) {
        QueryDataType type = resolveType(documentField.columnType);
        if (!field.type().equals(type) && !type.getConverter().canConvertTo(field.type().getTypeFamily())) {
            throw new IllegalStateException("Type " + field.type().getTypeFamily() + " of field " + field.name()
                    + " does not match db type " + type.getTypeFamily());
        }
    }

    Map<String, DocumentField> readFields(String[] externalNames,
                                          String dataConnectionName,
                                          Map<String, String> options,
                                          boolean stream) {
        String collectionName = externalNames.length == 2 ? externalNames[1] : externalNames[0];
        String databaseName = Options.getDatabaseName(nodeEngine, externalNames, dataConnectionName);

        Map<String, DocumentField> fields = new HashMap<>();
        try (MongoClient client = connect(dataConnectionName, options)) {
            requireNonNull(client);

            ResourceChecks resourceChecks = readExistenceChecksFlag(options);
            if (resourceChecks.isEverPerformed()) {
                checkDatabaseAndCollectionExists(client, databaseName, collectionName);
            }

            MongoDatabase database = client.getDatabase(databaseName);
            List<Document> collections = database.listCollections()
                                                 .filter(eq("name", collectionName))
                                                 .into(new ArrayList<>());
            if (collections.isEmpty()) {
                ArrayList<String> list = database.listCollectionNames().into(new ArrayList<>());
                throw new IllegalArgumentException("collection " + collectionName + " was not found, maybe you mean: "
                        + list);
            }
            Document collectionInfo = collections.get(0);
            Document properties = getIgnoringNulls(collectionInfo, "options", "validator", "$jsonSchema", "properties");
            if (properties != null) {
                for (Entry<String, Object> property : properties.entrySet()) {
                    Document props = (Document) property.getValue();
                    BsonType bsonType = getBsonType(props);

                    String key = property.getKey();
                    if (stream) {
                        key = "fullDocument." + key;
                    }
                    fields.put(key, new DocumentField(bsonType, key));
                }
            } else {
                // fall back to sampling
                ArrayList<Document> samples =
                        database.getCollection(collectionName).find().limit(1).into(new ArrayList<>());
                if (samples.isEmpty()) {
                    throw new IllegalStateException("Cannot infer schema of collection " + collectionName
                            + ", no documents found");
                }
                Document sample = samples.get(0);
                for (Entry<String, Object> entry : sample.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    String key = entry.getKey();
                    if (stream) {
                        key = "fullDocument." + key;
                    }
                    DocumentField field = new DocumentField(resolveTypeFromJava(entry.getValue()), key);
                    fields.put(key, field);
                }
            }
            if (stream) {
                fields.put("operationType", new DocumentField(BsonType.STRING, "operationType"));
                fields.put("resumeToken", new DocumentField(BsonType.STRING, "resumeToken"));
                fields.put("wallTime", new DocumentField(BsonType.DATE_TIME, "wallTime"));
                fields.put("ts", new DocumentField(BsonType.TIMESTAMP, "ts"));
                fields.put("clusterTime", new DocumentField(BsonType.TIMESTAMP, "clusterTime"));
            }
        }
        return fields;
    }

    @SuppressWarnings("SameParameterValue")
    private Document getIgnoringNulls(@Nonnull Document doc, @Nonnull String... options) {
        Document returned = doc;
        for (String option : options) {
            Object o = returned.get(option);
            if (o == null) {
                return null;
            }
            returned = (Document) o;
        }
        return returned;
    }

    private MongoClient connect(String dataConnectionName, Map<String, String> options) {
        if (dataConnectionName != null) {
            MongoDataConnection link = nodeEngine.getDataConnectionService().getAndRetainDataConnection(
                    dataConnectionName,
                    MongoDataConnection.class);
            try {
                return link.getClient();
            } finally {
                link.release();
            }
        } else {
            String connectionString = requireNonNull(options.get(CONNECTION_STRING_OPTION),
                    "Cannot connect to MongoDB, connectionString was not provided");

            return MongoClients.create(connectionString);
        }
    }

    @VisibleForTesting
    static class DocumentField {

        final String columnName;
        final BsonType columnType;

        DocumentField(BsonType columnType, String columnName) {
            this.columnType = requireNonNull(columnType);
            this.columnName = requireNonNull(columnName);
        }

        @Override
        public String toString() {
            return "MongoField{" +
                    "columnName='" + columnName + '\'' +
                    ", typeName='" + columnType + '\'' +
                    '}';
        }
    }
}
