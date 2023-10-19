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
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesInsertMongoSqlConnectorIT extends MongoSqlIT {
    private static final ObjectId EXAMPLE_OBJECT_ID = ObjectId.get();
    @Parameterized.Parameter
    public int no;

    @Parameterized.Parameter(1)
    public String bsonType;

    @Parameterized.Parameter(2)
    public String mappingType;

    @Parameterized.Parameter(3)
    public String sqlInsertValue;

    @Parameterized.Parameter(4)
    public Object valueInserted;

    @Parameterized.Parameter(5)
    public Object valueInMongo;

    @Parameterized.Parameter(6)
    public Object valueFromSql;

    @Parameterized.Parameters(name = "no {0}, bsonType {1}, mappingType {2}")
    public static Collection<Object[]> parameters() {
        LocalDateTime comparedDateTime = LocalDateTime.of(2022, 12, 30, 23, 59, 59);
        ZonedDateTime dateTimeUtc = comparedDateTime.atZone(UTC);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String dateTimeString = dateTimeUtc.withZoneSameInstant(systemDefault()).format(formatter);

        String dateTimeStringTz = "cast ('" + dateTimeUtc.withZoneSameInstant(systemDefault()).format(ISO_OFFSET_DATE_TIME)
                + "' as timestamp with time zone)";
        return asList(new Object[][]{
                {1, "string", "VARCHAR", "'dummy'", "dummy", "dummy", "dummy"},
                {2, "bool", "BOOLEAN", "TRUE", true, true, true},
                {3, "int", "TINYINT", "1", (byte) 1, 1, (byte) 1},
                {4, "int", "SMALLINT", "2", (short) 2, 2, (short) 2},
                {5, "int", "INTEGER", "3", 3, 3, 3},
                {6, "long", "BIGINT", "4", 4L, 4L, 4L},
                {7, "decimal", "DECIMAL", "1.12345", new BigDecimal("1.12345"),
                        new Decimal128(new BigDecimal("1.12345")), new BigDecimal("1.12345")},
                {8, "double", "REAL", "1.5", 1.5f, 1.5d, 1.5f},
                {9, "double", "DOUBLE", "1.8", 1.8, 1.8d, 1.8},
                {10, "date", "DATE", "'2022-12-30'", LocalDate.of(2022, 12, 30),
                        new Date(LocalDate.parse("2022-12-30").atStartOfDay(ZoneId.of("UTC")).toEpochSecond() * 1000),
                        LocalDate.of(2022, 12, 30)
                },
                {11, "date", "TIMESTAMP", "'" + dateTimeString + "'",
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime(),
                        new Date(dateTimeUtc.toInstant().toEpochMilli()),
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime()
                },
                {12, "timestamp", "TIMESTAMP", "'" + dateTimeString + "'",
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime(),
                        new BsonTimestamp((int) dateTimeUtc.toEpochSecond(), 0),
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toLocalDateTime(),
                },
                {13, "date", "TIMESTAMP WITH TIME ZONE", dateTimeStringTz,
                        dateTimeUtc.withZoneSameInstant(systemDefault()),
                        new Date(dateTimeUtc.toInstant().toEpochMilli()),
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toOffsetDateTime(),
                },
                {14, "timestamp", "TIMESTAMP WITH TIME ZONE", dateTimeStringTz,
                        dateTimeUtc.withZoneSameInstant(systemDefault()),
                        new BsonTimestamp((int) dateTimeUtc.toEpochSecond(), 0),
                        dateTimeUtc.withZoneSameInstant(systemDefault()).toOffsetDateTime(),
                },
                {15, "objectId", "OBJECT", null, EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID },
                {16, "object", "JSON", "JSON_OBJECT('test':'abc')", new HazelcastJsonValue("{\"test\": \"abc\"}"),
                        new Document("test", "abc"), new HazelcastJsonValue("{\"test\": \"abc\"}") }
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

        if (sqlInsertValue != null) {
            execute("INSERT INTO " + mappingName + " VALUES(0, " + sqlInsertValue + ")");
        }
        execute("INSERT INTO " + mappingName + " VALUES(1, ?)", valueInserted);

        MongoCollection<Document> collection = database.getCollection(collectionName);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        List<Row> fromMongo = list.stream()
                               .map(d -> new Row(d.getInteger("id"), d.get("table_column")))
                               .collect(Collectors.toList());

        if (sqlInsertValue == null) {
            assertThat(fromMongo).containsExactlyInAnyOrder(new Row(1, valueInMongo));
            assertRowsAnyOrder("select * from " + mappingName, new Row(1, valueFromSql));
        } else {
            assertThat(fromMongo).containsExactlyInAnyOrder(
                    new Row(0, valueInMongo),
                    new Row(1, valueInMongo)
            );
            assertRowsAnyOrder("select * from " + mappingName,
                    new Row(0, valueFromSql),
                    new Row(1, valueFromSql)
            );
        }
    }

}
