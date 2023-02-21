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

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesInsertMongoSqlConnectorTest extends MongoSqlTest {
    private static final ObjectId EXAMPLE_OBJECT_ID = ObjectId.get();

    @Parameterized.Parameter
    public String type;

    @Parameterized.Parameter(1)
    public String mappingType;

    @Parameterized.Parameter(2)
    public String sqlValue;

    @Parameterized.Parameter(3)
    public Object javaValue;

    @Parameterized.Parameter(4)
    public Object mongoValue;

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, sqlValue:{2}, javaValue:{3}, mongoValue:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"string", "VARCHAR", "'dummy'", "dummy", "dummy"},
                {"bool", "BOOLEAN", "TRUE", true, true},
                {"int", "TINYINT", "1", (byte) 1, 1},
                {"int", "SMALLINT", "2", (short) 2, 2},
                {"int", "INTEGER", "3", 3, 3},
                {"long", "BIGINT", "4", 4L, 4L},
                {"decimal", "DECIMAL", "1.12345", new BigDecimal("1.12345"), new Decimal128(new BigDecimal("1.12345"))},
                {"double", "REAL", "1.5", 1.5f, 1.5d},
                {"double", "DOUBLE", "1.8", 1.8, 1.8d},
                {"date", "DATE", "'2022-12-30'", LocalDate.of(2022, 12, 30),
                        new Date(LocalDate.parse("2022-12-30").atStartOfDay(ZoneId.of("UTC")).toEpochSecond() * 1000)},
                {"date", "TIMESTAMP", "'2022-12-30 23:59:59'",
                        LocalDateTime.of(2022, 12, 30, 23, 59, 59),
                        new Date(Timestamp.valueOf("2022-12-30 23:59:59").getTime())},
                {"objectId", "OBJECT", null, EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID}
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
                        "        \"table_column\": { \"bsonType\": \"" + type + "\" }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        database.createCollection(collectionName, options);

        String mappingName = "mapping_" + randomName();
        execute("CREATE MAPPING " + mappingName
                + " EXTERNAL NAME " + collectionName
                + " ("
                + "id INT, "
                + "table_column " + mappingType
                + ") "
                + "TYPE MongoDB " + options()
        );

        if (sqlValue != null) {
            execute("INSERT INTO " + mappingName + " VALUES(0, " + sqlValue + ")");
        }
        execute("INSERT INTO " + mappingName + " VALUES(1, ?)", javaValue);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        List<Row> fromMongo = list.stream()
                               .map(d -> new Row(d.getInteger("id"), d.get("table_column")))
                               .collect(Collectors.toList());

        if (sqlValue == null) {
            assertThat(fromMongo).containsExactlyInAnyOrder(new Row(1, mongoValue));
            assertRowsAnyOrder("select * from " + mappingName, new Row(1, javaValue));
        } else {
            assertThat(fromMongo).containsExactlyInAnyOrder(
                    new Row(0, mongoValue),
                    new Row(1, mongoValue)
            );
            assertRowsAnyOrder("select * from " + mappingName,
                    new Row(0, javaValue),
                    new Row(1, javaValue)
            );
        }
    }

}
