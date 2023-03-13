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
package com.hazelcast.jet.mongodb.datalink;

import com.hazelcast.datalink.DataLinkResource;
import com.hazelcast.jet.mongodb.AbstractMongoTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static com.hazelcast.jet.mongodb.datalink.MongoDataLink.mongoDataLinkConf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MongoDataLinkTest extends AbstractMongoTest {
    private MongoDataLink dataLink;
    private String connectionString;

    @Before
    public void setup() {
        this.connectionString = mongoContainer.getConnectionString();
    }

    @After
    public void cleanup() {
        if (dataLink != null) {
            dataLink.destroy();
            dataLink = null;
        }
    }

    @Test
    public void should_return_same_link_when_shared() {
        dataLink = new MongoDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released() {
        dataLink = new MongoDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        dataLink.release();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_return_collections_when_listResources() {
        dataLink = new MongoDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoDatabase db1 = client1.getDatabase("test");
        MongoDatabase db2 = client1.getDatabase("test2");

        db1.createCollection("col1");
        db1.createCollection("col2");
        db2.createCollection("col3");

        assertThat(dataLink.listResources()).contains(
                new DataLinkResource("collection", "test.col1"),
                new DataLinkResource("collection", "test.col2"),
                new DataLinkResource("collection", "test2.col3")
        );
    }

    @Test
    public void should_return_new_link_when_not_shared() {
        dataLink = new MongoDataLink(mongoDataLinkConf("mongo", connectionString).setShared(false));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isNotSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released_when_not_shared() {
        dataLink = new MongoDataLink(mongoDataLinkConf("mongo", connectionString).setShared(false));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        assertClosed(client1);
        assertClosed(client2);
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
