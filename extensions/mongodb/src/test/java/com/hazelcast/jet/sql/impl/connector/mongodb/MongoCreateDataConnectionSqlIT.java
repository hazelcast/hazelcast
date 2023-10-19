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

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.starter.ReflectionUtils;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MongoCreateDataConnectionSqlIT extends MongoSqlIT {

    @Test
    public void createsConnection() {
        String dlName = randomName();
        try (MongoClient client = MongoClients.create(connectionString)) {
            client.getDatabase("test1").createCollection("test2");
        }
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName + " TYPE Mongo SHARED " + options());

        DataConnection dataConnection = getNodeEngineImpl(
                instance()).getDataConnectionService().getAndRetainDataConnection(dlName, MongoDataConnection.class);

        assertThat(dataConnection).isNotNull();
        assertThat(dataConnection.getConfig().getType()).isEqualTo("mongo");

        try (SqlResult result = instance().getSql().execute("SHOW RESOURCES FOR " + dlName)) {
            boolean hasCollectionWeWanted = false;
            for (SqlRow row : result) {
                if (row.getObject("name").equals("\"test1\".\"test2\"")) {
                    hasCollectionWeWanted = true;
                }
            }
            assertThat(hasCollectionWeWanted).isTrue();
        }
    }

    @Test
    public void createsConnectionWithPoolSize() throws IllegalAccessException {
        String dlName = randomName();
        String dbName = randomName();
        String colName = randomName();
        try (MongoClient client = MongoClients.create(connectionString)) {
            client.getDatabase(dbName).createCollection(colName);
        }
        String options = String.format("OPTIONS ('connectionString' = '%s', 'idColumn' = 'id', " +
                        "'connectionPoolMinSize' = '1337', 'connectionPoolMaxSize' = '2023') ",
                connectionString);
        instance().getSql().executeUpdate("CREATE DATA CONNECTION " + dlName + " TYPE Mongo SHARED " + options);

        MongoDataConnection dataConnection = getNodeEngineImpl(
                instance()).getDataConnectionService().getAndRetainDataConnection(dlName, MongoDataConnection.class);

        assertThat(dataConnection).isNotNull();

        try (MongoClient client = dataConnection.getClient()) {
            var impl = ReflectionUtils.getFieldValueReflectively(client, "delegate");
            var settings = (MongoClientSettings) ReflectionUtils.getFieldValueReflectively(impl, "settings");
            assertThat(settings.getConnectionPoolSettings().getMinSize()).isEqualTo(1337);
            assertThat(settings.getConnectionPoolSettings().getMaxSize()).isEqualTo(2023);
        }
    }

    @Test
    public void createsConnectionEvenWhenUnreachable_shared() {
        testCreatesConnectionEvenWhenUnreachable(true);
    }

    @Test
    public void createsConnectionEvenWhenUnreachable_unshared() {
        testCreatesConnectionEvenWhenUnreachable(false);
    }

    private void testCreatesConnectionEvenWhenUnreachable(boolean shared) {
        String dataConnName = randomName();
        String options = String.format("OPTIONS ('connectionString' = '%s', 'database' = 'fakeNonExisting') ",
                "mongodb://non-existing-address:1234/?connectTimeoutMS=20&socketTimeoutMS=20&serverSelectionTimeoutMS=20");

        String sharedString = shared ? " SHARED " : " ";
        instance().getSql().execute("CREATE DATA CONNECTION " + dataConnName + " TYPE Mongo " + sharedString + options)
                .close();

        DataConnection dataConnection = getNodeEngineImpl(
                instance()).getDataConnectionService().getAndRetainDataConnection(dataConnName, MongoDataConnection.class);

        assertThat(dataConnection).isNotNull();
        assertThat(dataConnection.getConfig().getType()).isEqualTo("mongo");

        Exception e = assertThrows(HazelcastSqlException.class, () -> {
            instance().getSql().execute("CREATE MAPPING test_" + shared + " data connection " + dataConnName)
                    .close();
        });
        assertThat(e.getMessage()).contains("address=non-existing-address:1234");
    }
}
