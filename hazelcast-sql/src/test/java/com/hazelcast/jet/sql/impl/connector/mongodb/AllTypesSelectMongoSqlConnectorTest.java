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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesSelectMongoSqlConnectorTest extends MongoSqlTest {

    @Parameterized.Parameter(0)
    public String bsonType;

    @Parameterized.Parameter(1)
    public String mappingType;

    @Parameterized.Parameter(2)
    public Object value;

    @Parameterized.Parameter(3)
    public Object expected;

    @Parameterized.Parameters(name = "mappingType:{0}, value:{1}, expected:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"string", "VARCHAR", "dummy", "dummy"},
                {"string", "BOOLEAN", "TRUE", true},
                {"bool", "BOOLEAN", true, true},
                {"int", "TINYINT", (byte) 1, (byte) 1},
                {"int", "SMALLINT", (short) 2, (short) 2},
                {"int", "INTEGER", 3, 3},
                {"long", "BIGINT", 4L, 4L},
                {"string", "TINYINT", "1", (byte) 1},
                {"string", "SMALLINT", "2", (short) 2},
                {"string", "INTEGER", "3", 3},
                {"string", "BIGINT", "4", 4L},
                {"double", "DECIMAL", 1.5d, new BigDecimal("1.5")},
                {"double", "REAL", 1.5f, 1.5f},
                {"double", "DOUBLE", 1.8d, 1.8d},
                {"date", "DATE", LocalDate.parse("2022-12-30"), LocalDate.of(2022, 12, 30)},
                {"date", "TIMESTAMP", atUtc(), OffsetDateTime.of(
                        LocalDate.of(2022, 12, 30),
                        LocalTime.of(23, 59, 59),
                        ZoneOffset.UTC).toLocalDateTime()}
        });
    }

    private static Timestamp atUtc() {
        return Timestamp.valueOf(OffsetDateTime.of(
                LocalDate.of(2022, 12, 30),
                LocalTime.of(23, 59, 59),
                ZoneOffset.UTC).toLocalDateTime());
    }

    @Test
    public void selectRowWithAllTypes() {
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
                        "        \"document_column\": { \"bsonType\": \"" + bsonType + "\" }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n"
        ));
        options.validationOptions(validationOptions);
        database.createCollection(collectionName, options);
        database.getCollection(collectionName).insertOne(new Document("document_column", value));

        String mappingName = "mapping_" + randomName();
        execute("CREATE MAPPING " + mappingName
                + " EXTERNAL NAME " + collectionName
                + " (document_column " + mappingType + ") "
                + "TYPE MongoDB " + options()
        );

        assertRowsAnyOrder("SELECT * FROM " + mappingName, new Row(expected));

        // todo: mongo has no automatic coertion here
//        assertRowsAnyOrder("SELECT * FROM " + mappingName + " WHERE document_column = ?",
//                newArrayList(expected),
//                new Row(expected)
//        );
    }

}
