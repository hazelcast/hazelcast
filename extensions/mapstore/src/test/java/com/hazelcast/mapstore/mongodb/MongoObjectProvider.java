/*
 * Copyright 2025 Hazelcast Inc.
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
package com.hazelcast.mapstore.mongodb;

import com.hazelcast.test.jdbc.TestDatabaseRecordProvider;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.Column.col;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.INT;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

public class MongoObjectProvider implements TestDatabaseRecordProvider {

    protected MongoDatabaseProvider databaseProvider;

    public MongoObjectProvider(MongoDatabaseProvider databaseProvider) {
        this.databaseProvider = databaseProvider;
    }

    @Override
    public ObjectSpec createObject(String objectName, boolean useQuotedNames) {
        String idName = useQuotedNames ? "person-id" : "id";
        var spec = new ObjectSpec(objectName, col(idName, INT, true), col("name", STRING));
        createObject(spec);
        return spec;
    }

    @Override
    @SuppressWarnings("OperatorWrap")
    public void createObject(ObjectSpec spec) {
        String objectName = spec.name;
        List<Column> columns = spec.columns;
        String propertiesString = columns.stream()
                .map(col -> "\"" + col.name + "\": { \"bsonType\": \"" +  resolveType(col.type) + "\" },")
                .collect(Collectors.joining("\n"));
        CreateCollectionOptions options = new CreateCollectionOptions();
        ValidationOptions validationOptions = new ValidationOptions();
        validationOptions.validator(BsonDocument.parse(
                "{\n" +
                        "    $jsonSchema: {\n" +
                        "      bsonType: \"object\",\n" +
                        "      title: \"Object Validation\",\n" +
                        "      properties: {" +
                        propertiesString +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        databaseProvider.database().createCollection(objectName, options);
    }

    private String resolveType(ColumnType type) {
        switch (type) {
            case INT: return "int";
            case STRING: return "string";
            default: throw new UnsupportedOperationException("type " + type + " is not supported");
        }
    }

    @Override
    public void insertItems(ObjectSpec spec, int count) {
        var objectName = spec.name;
        MongoCollection<Document> collection = databaseProvider.database().getCollection(objectName);
        List<Document> toInsert = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Document doc = new Document();
            int col = 0;
            for (Column column : spec.columns) {
                switch (column.type) {
                    case INT: doc.append(column.name, i + col); break;
                    case STRING: doc.append(column.name, String.format("%s-%d", column.name, i)); break;
                    default: throw new UnsupportedOperationException();
                }
                col++;
            }
            toInsert.add(doc);
        }
        collection.insertMany(toInsert);
    }

    @Override
    public TestDatabaseProvider provider() {
        return databaseProvider;
    }

    @Override
    public void assertRows(String objectName, List<List<Object>> rows) {
        MongoCollection<Document> collection = databaseProvider.database().getCollection(objectName);
        ArrayList<Document> documents = collection.find().into(new ArrayList<>());
        List<List<Object>> actualRows = new ArrayList<>();
        for (Document document : documents) {
            ArrayList<Object> row = new ArrayList<>(document.values());
            row.remove(0); // _id column
            actualRows.add(row);
        }
        assertThat(actualRows)
                .containsExactlyInAnyOrderElementsOf(rows);
    }
}
