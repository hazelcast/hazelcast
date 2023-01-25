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
package com.hazelcast.jet.mongodb.sql;

import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonType;
import org.bson.Document;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.mongodb.sql.Options.COLLECTION_NAME_OPTION;
import static com.hazelcast.jet.mongodb.sql.Options.CONNECTION_STRING_OPTION;
import static com.hazelcast.jet.mongodb.sql.Options.DATABASE_NAME_OPTION;
import static com.hazelcast.jet.mongodb.sql.BsonTypes.resolveTypeByName;
import static com.hazelcast.jet.mongodb.sql.BsonTypes.resolveTypeFromJava;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Objects.requireNonNull;

class FieldResolver {

    List<MappingField> resolveFields(
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        Map<String, MongoField> dbFields = readFields(options);

        List<MappingField> resolvedFields = new ArrayList<>();
        if (userFields.isEmpty()) {
            for (MongoField mongoField : dbFields.values()) {
                try {
                    MappingField mappingField = new MappingField(
                            mongoField.columnName,
                            resolveType(mongoField.columnType)
                    );
                    mappingField.setPrimaryKey(mongoField.columnName.equalsIgnoreCase("_id"));
                    resolvedFields.add(mappingField);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Could not load column class " + mongoField.columnType, e);
                }
            }
        } else {
            for (MappingField f : userFields) {
                String nameInMongo = f.externalName() == null ? f.name() : f.externalName();
                MongoField mongoField = dbFields.get(nameInMongo);
                if (mongoField == null) {
                    throw new IllegalStateException("Could not resolve field with name " + nameInMongo);
                }
                MappingField mappingField = new MappingField(f.name(), f.type(), nameInMongo);
                mappingField.setPrimaryKey(mongoField.columnName.equalsIgnoreCase("_id"));
                validateType(f, mongoField);
                resolvedFields.add(mappingField);
            }
        }
        return resolvedFields;
    }

    private QueryDataType resolveType(BsonType columnType) {
        switch (columnType) {
            case INT32: return QueryDataType.INT;
            case INT64: return QueryDataType.BIGINT;
            case DOUBLE: return QueryDataType.DOUBLE;
            case TIMESTAMP:
            case DATE_TIME:
                return QueryDataType.TIMESTAMP;
            case STRING:
            case OBJECT_ID:
                return QueryDataType.VARCHAR;
            case DECIMAL128: return QueryDataType.DECIMAL;
            default:  throw new UnsupportedOperationException("Cannot resolve type for BSON type " + columnType);
        }
    }

    private void validateType(MappingField field, MongoField mongoField) {
        QueryDataType type = resolveType(mongoField.columnType);
        if (!field.type().equals(type) && !type.getConverter().canConvertTo(field.type().getTypeFamily())) {
            throw new IllegalStateException("Type " + field.type().getTypeFamily() + " of field " + field.name()
                    + " does not match db type " + type.getTypeFamily());
        }
    }

    Map<String, MongoField> readFields(Map<String, String> options) {
        Map<String, MongoField> fields = new HashMap<>();
        try (MongoClient client = connect(options)) {
            String databaseName = requireNonNull(options.get(DATABASE_NAME_OPTION),
                    DATABASE_NAME_OPTION + " option must be provided");
            String collectionName = options.get(COLLECTION_NAME_OPTION);

            MongoDatabase database = client.getDatabase(databaseName);
            List<Document> collections = database.listCollections()
                                                      .filter(eq("name", collectionName))
                                                    .into(new ArrayList<>());
            if (collections.isEmpty()) {
                throw new IllegalArgumentException("collection " + collectionName + " was not found");
            }
            Document collectionInfo = collections.get(0);
            Document properties = getIgnoringNulls(collectionInfo, "options", "validator", "$jsonSchema", "properties");
            if (properties != null) {
                for (Entry<String, Object> property : properties.entrySet()) {
                       Document props = (Document) property.getValue();
                    String bsonTypeName = (String) props.get("bsonType");
                    BsonType bsonType = resolveTypeByName(bsonTypeName);

                    fields.put(property.getKey(), new MongoField(bsonType, property.getKey()));
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
                    MongoField field = new MongoField(resolveTypeFromJava(entry.getValue()), entry.getKey());
                    fields.put(entry.getKey(), field);
                }
            }
        }
        return fields;
    }

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

    private MongoClient connect(Map<String, String> options) {
        if (options.containsKey("externalDataSourceRef")) {
            // todo: external data source support
            throw new UnsupportedOperationException("not yet supported");
        } else {
            String connectionString = requireNonNull(options.get(CONNECTION_STRING_OPTION),
                    "Cannot connect to MongoDB, connectionString was not provided");

            return MongoClients.create(connectionString);
        }
    }

    static class MongoField {

        final BsonType columnType;
        final String columnName;

        MongoField(BsonType columnType, String columnName) {
            this.columnType = requireNonNull(columnType);
            this.columnName = requireNonNull(columnName);
        }

        @Override
        public String toString() {
            return "DbField{" +
                    "name='" + columnName + '\'' +
                    ", typeName='" + columnType + '\'' +
                    '}';
        }
    }
}
