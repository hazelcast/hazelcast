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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BasicBSONObject;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collection;

import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AllTypesSelectMongoSqlConnectorIT extends MongoSqlIT {
    private static final ObjectId EXAMPLE_OBJECT_ID = ObjectId.get();

    @Parameterized.Parameter(0)
    public String bsonType;

    @Parameterized.Parameter(1)
    public String mappingType;

    @Parameterized.Parameter(2)
    public Object value;

    @Parameterized.Parameter(3)
    public Object expected;

    @Parameterized.Parameters(name = "bsonType:{0}, mappingType:{1}, value:{2}, expected:{3}")
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
                {"date", "DATE", atUtc().toLocalDate(), atLocal().toLocalDate()},
                {"date", "TIMESTAMP", atUtc(), atLocal()},
                {"timestamp", "TIMESTAMP", atLocalTimestamp(), atLocal()},
                {"minKey", "OBJECT", new MinKey(), new MinKey()},
                {"maxKey", "OBJECT", new MaxKey(), new MaxKey()},
                {"objectId", "OBJECT", EXAMPLE_OBJECT_ID, EXAMPLE_OBJECT_ID},
                {"object", "OBJECT", new BasicBSONObject(), new Document()},
                {"decimal", "DECIMAL", new Decimal128(new BigDecimal("1.23")), new BigDecimal("1.23")},
                {"javascript", "VARCHAR", new BsonJavaScript("{}"), "{}"},
                {"javascriptWithScope", "OBJECT", new BsonJavaScriptWithScope("{}", new BsonDocument()),
                        new CodeWithScope("{}", new Document())},
                {"javascriptWithScope", "OBJECT", new CodeWithScope("{}", new Document()),
                        new CodeWithScope("{}", new Document())},
                {"array", "OBJECT", new BsonArray(asList(new BsonString("1"), new BsonString("2"))),
                        asList("1", "2")},
                {"array", "OBJECT", asList("1", "2"), asList("1", "2")},
                {"regex", "OBJECT", new BsonRegularExpression(".*"), new BsonRegularExpression(".*")},
                {"object", "json", new Document("test", "abc"), new HazelcastJsonValue("{\"test\": \"abc\"}")}
        });
    }

    private static LocalDateTime atLocal() {
        return LocalDateTime.of(
                LocalDate.of(2022, 12, 30),
                LocalTime.of(23, 59, 59));
    }
    private static BsonTimestamp atLocalTimestamp() {
        return new BsonTimestamp((int) atLocal().atZone(systemDefault()).toEpochSecond(), 0);
    }

    /**
     * {@link #atLocal()} converted to UTC.
     * If system timezone is UTC, result is identical; if for example system timezone is CET, then result will be
     * 1 hour before.
     */
    private static LocalDateTime atUtc() {
        ZoneId zoneId = ZoneId.systemDefault();
        return atLocal().atZone(zoneId).withZoneSameInstant(UTC).toLocalDateTime();
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
                + " EXTERNAL NAME " + databaseName + "." + collectionName
                + " (document_column " + mappingType + ") "
                + "TYPE Mongo " + options()
        );

        assertRowsAnyOrder("SELECT * FROM " + mappingName, new Row(expected));
    }

}
