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
package com.hazelcast.jet.mongodb.dataconnection;

import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.jet.mongodb.AbstractMongoTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import static com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection.mongoDataConnectionConf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MongoDataConnectionTest extends AbstractMongoTest {
    private MongoDataConnection dataConnection;
    private String connectionString;

    @Before
    public void setup() {
        this.connectionString = mongoContainer.getConnectionString();
    }

    @After
    public void cleanup() {
        if (dataConnection != null) {
            dataConnection.destroy();
            dataConnection = null;
        }
    }

    @Test
    public void should_return_same_link_when_shared() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString));

        MongoClient client1 = dataConnection.getClient();
        MongoClient client2 = dataConnection.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString));

        MongoClient client1 = dataConnection.getClient();
        MongoClient client2 = dataConnection.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        dataConnection.release();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_return_resource_types() {
        // given
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString));

        // when
        Collection<String> resourcedTypes = dataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("collection", "changestream");
    }

    @Test
    public void should_return_collections_when_listResources() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString));

        MongoClient client1 = dataConnection.getClient();
        MongoDatabase db1 = client1.getDatabase("test");
        MongoDatabase db2 = client1.getDatabase("test2");

        db1.createCollection("col1");
        db1.createCollection("col2");
        db2.createCollection("col3");

        assertThat(dataConnection.listResources()).contains(
                new DataConnectionResource("Collection", "test", "col1"),
                new DataConnectionResource("Collection", "test", "col2"),
                new DataConnectionResource("Collection", "test2", "col3")
        );
    }

    @Test
    public void should_return_new_link_when_not_shared() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString).setShared(false));

        MongoClient client1 = dataConnection.getClient();
        MongoClient client2 = dataConnection.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isNotSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released_when_not_shared() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString).setShared(false));

        MongoClient client1 = dataConnection.getClient();
        MongoClient client2 = dataConnection.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_configure_pool_size() {
        dataConnection = new MongoDataConnection(mongoDataConnectionConf("mongo", connectionString)
                .setShared(false)
                .setProperty(MongoDataConnection.CONNECTION_POOL_MIN, "1337")
                .setProperty(MongoDataConnection.CONNECTION_POOL_MAX, "2023"));

        try (MongoClient client = dataConnection.getClient()) {
            var asCloseable = (CloseableMongoClient) client;
            var impl = (MongoClientImpl) asCloseable.delegate;
            assertThat(impl.getSettings().getConnectionPoolSettings().getMinSize()).isEqualTo(1337);
            assertThat(impl.getSettings().getConnectionPoolSettings().getMaxSize()).isEqualTo(2023);
        }
    }

    private void assertNotClosed(MongoClient client) {
        client.listDatabases().into(new ArrayList<>());
    }
    private void assertClosed(MongoClient client) {
        try {
            client.listDatabases().into(new ArrayList<>());
            fail("Should fail if client is closed");
        } catch (IllegalStateException e) {
            if (!e.getMessage().equals("state should be: open")) {
                throw e;
            }
        }
    }

}
