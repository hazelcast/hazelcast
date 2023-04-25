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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LogListener;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class MongoBatchSqlConnectorDeleteTest extends MongoSqlTest {

    @Parameterized.Parameter(0)
    public boolean includeIdInMapping;
    @Parameterized.Parameter(1)
    public boolean idFirstInMapping;

    @Parameterized.Parameters(name = "includeIdInMapping:{0}, idFirstInMapping:{1}")
    public static Object[][] parameters() {
        return new Object[][]{
                { true, true },
                { true, false },
                { false, true }
        };
    }

    @Test
    public void deletes_inserted_item() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp2").append("lastName", "temp2")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp3").append("lastName", "temp3")
                                                          .append("jedi", true));

        createMapping(includeIdInMapping, idFirstInMapping);

        execute("delete from " + collectionName + " where firstName = ?", "temp");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(2);
        execute("delete from " + collectionName);
        list = collection.find().into(new ArrayList<>());
        assertThat(list).isEmpty();
    }

    @Test
    public void deletes_inserted_item_byId() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp2").append("lastName", "temp2")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "temp3").append("lastName", "temp3")
                                                          .append("jedi", true));

        createMapping(true, idFirstInMapping);

        execute("delete from " + collectionName + " where id = ?", objectId);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(2);
        execute("delete from " + collectionName);
        list = collection.find().into(new ArrayList<>());
        assertThat(list).isEmpty();
    }

    @Test
    public void deletes_selfRef_in_where() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "someValue")
                                                          .append("lastName", "someValue")
                                                          .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "someValue2")
                                                                .append("lastName", "someValue2")
                                                                .append("jedi", true));
        collection.insertOne(new Document("_id", ObjectId.get()).append("firstName", "someValue3")
                                                                .append("lastName", "otherValue")
                                                                .append("jedi", true));

        createMapping(includeIdInMapping, idFirstInMapping);

        execute("delete from " + collectionName + " where firstName = lastName");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(1);
        assertThat(list.get(0).getString("lastName")).isEqualTo("otherValue");
    }

    @Test
    public void deletes_unsupportedExpr() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));

        createMapping(includeIdInMapping, idFirstInMapping);

        execute("delete from " + collectionName + " where cast(jedi as varchar) = ?", "true");
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(0);
    }

    @Test
    public void deletes_all() {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ObjectId objectId = ObjectId.get();
        collection.insertOne(new Document("_id", objectId).append("firstName", "temp").append("lastName", "temp")
                                                          .append("jedi", true));

        createMapping(includeIdInMapping, idFirstInMapping);

        execute("delete from " + collectionName);
        ArrayList<Document> list = collection.find().into(new ArrayList<>());
        assertThat(list).hasSize(0);
    }

    private void createMapping(boolean includeIdInMapping, boolean idFirst) {
        if (idFirst) {
            execute("CREATE MAPPING " + collectionName + " external name \"" + databaseName + "\".\"" + collectionName + "\" \n("
                    + (includeIdInMapping ? " id OBJECT external name _id, " : "")
                    + " firstName VARCHAR, \n"
                    + " lastName VARCHAR, \n"
                    + " jedi BOOLEAN \n"
                    + ") \n"
                    + "TYPE Mongo \n"
                    + "OPTIONS (\n"
                    + "    'connectionString' = '" + mongoContainer.getConnectionString() + "' \n"
                    + ")"
            );
        } else {
            execute("CREATE MAPPING " + collectionName + " external name \"" + databaseName + "\".\"" + collectionName + "\" \n("
                    + " firstName VARCHAR, \n"
                    + " lastName VARCHAR, \n"
                    + (includeIdInMapping ? " id OBJECT external name _id, " : "")
                    + " jedi BOOLEAN \n"
                    + ") \n"
                    + "TYPE Mongo \n"
                    + "OPTIONS (\n"
                    + "    'connectionString' = '" + mongoContainer.getConnectionString() + "' \n"
                    + ")"
            );
        }
    }
}
