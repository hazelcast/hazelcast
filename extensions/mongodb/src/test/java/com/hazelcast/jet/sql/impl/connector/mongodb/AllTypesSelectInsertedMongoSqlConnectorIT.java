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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.mongodb.impl.MongoUtilities.localDateTimeToTimestamp;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesSelectInsertedMongoSqlConnectorIT extends MongoSqlIT {
    private static final ObjectId EXAMPLE_OBJECT_ID = ObjectId.get();
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    @Parameterized.Parameter
    public int no;

    @Parameterized.Parameter(1)
    public String bsonType;

    @Parameterized.Parameter(2)
    public String mappingType;

    @Parameterized.Parameter(3)
    public Object valueInserted;

    @Parameterized.Parameter(4)
    public Object valueExpected;

    @Parameterized.Parameters(name = "no {0}, bsonType {1}, mappingType {2}, value {3}")
    public static Collection<Object[]> parameters() {
        LocalDateTime comparedDateTime = LocalDateTime.of(2022, 12, 30, 23, 59, 59);
        ZonedDateTime dateTimeUtc = comparedDateTime.atZone(UTC);

        BigDecimal objectIdAsDecimal = new BigDecimal(new BigInteger(EXAMPLE_OBJECT_ID.toHexString(), 16));
        String testJson = "{\"test\": \"abc\"}";
        return asList(new Object[][]{
                {1, "string", "VARCHAR", "dummy", "dummy"},
                {2, "bool", "BOOLEAN", true, true},
                {3, "int", "TINYINT", (byte) 1, (byte) 1},
                {4, "int", "SMALLINT", (short) 2, (short) 2},
                {5, "int", "INTEGER", 3, 3},
                {6, "int", "varchar", 3, "3"},
                {7, "long", "BIGINT", 4L, 4L},
                {8, "long", "DECIMAL", 4L, new BigDecimal(4)},
                {9, "decimal", "DECIMAL", new BigDecimal("1.12345"), new BigDecimal("1.12345")},
                {10, "double", "REAL", 1.5f, 1.5f},
                {11, "double", "DOUBLE", 1.8, 1.8},
                {12, "date", "DATE", LocalDate.of(2022, 12, 30), LocalDate.of(2022, 12, 30)},
                {13, "timestamp", "TIMESTAMP",
                        localDateTimeToTimestamp(dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime()),
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime()
                },
                {14, "objectId", "OBJECT", EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID },
                {15, "objectId", "VARCHAR", EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID.toHexString() },
                {16, "objectId", "DECIMAL", EXAMPLE_OBJECT_ID, objectIdAsDecimal},
                {17, "object", "JSON", new Document("test", "abc"), new HazelcastJsonValue(testJson) },
                {18, "string", "JSON", testJson, new HazelcastJsonValue(testJson) },
                {19, "object", "VARCHAR", new Document("test", "abc"), testJson},
                {20, "object", "VARCHAR", new BsonDocument("test", new BsonString("abc")), testJson}
        });
    }

    @Test
    public void insertRowWithAllTypes() {
        String collectionName = randomName();

        CreateCollectionOptions options = new CreateCollectionOptions();
        ValidationOptions validationOptions = new ValidationOptions();
        validationOptions.validator(BsonDocument.parse(
                "{\n" +
                        "    $jsonSchema: {\n" +
                        "      bsonType: \"object\",\n" +
                        "      title: \"Object Validation\",\n" +
                        "      properties: {" +
                        "        \"id\": { \"bsonType\": \"int\" },\n" +
                        "        \"table_column\": { \"bsonType\": \"" + bsonType + "\" }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        database.createCollection(collectionName, options);

        String mappingName = "mapping_" + randomName();
        execute("CREATE MAPPING " + mappingName
                + " EXTERNAL NAME " + databaseName + "." + collectionName
                + " ("
                + "id INT, "
                + "table_column " + mappingType
                + ") "
                + "TYPE Mongo " + options()
        );

        MongoCollection<Document> sourceCollection = database.getCollection(collectionName);
        int insertedId = NEXT_ID.get();
        sourceCollection.insertOne(new Document("id", insertedId).append("table_column", valueInserted));

        execute("INSERT INTO " + mappingName + "(id, table_column) values (?, ?)", insertedId + 1, valueExpected);

        execute("INSERT INTO " + mappingName + "(id, table_column) SELECT id + 2, table_column from " + mappingName
                + " where id = " + insertedId);

        try (SqlResult sqlResult = sqlService.execute("select * from " + mappingName)) {
            List<Row> result = new ArrayList<>();

            sqlResult.forEach(r -> result.add(new Row(r)));

            assertThat(result).containsExactlyInAnyOrder(
                    new Row(insertedId, valueExpected),
                    new Row(insertedId + 1, valueExpected),
                    new Row(insertedId + 2, valueExpected)
            );
        }
    }

}
