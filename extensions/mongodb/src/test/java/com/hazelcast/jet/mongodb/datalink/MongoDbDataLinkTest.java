package com.hazelcast.jet.mongodb.datalink;

import com.hazelcast.datalink.Resource;
import com.hazelcast.jet.mongodb.AbstractMongoDBTest;
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

import static com.hazelcast.jet.mongodb.datalink.MongoDbDataLink.mongoDataLinkConf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MongoDbDataLinkTest extends AbstractMongoDBTest {
    private MongoDbDataLink dataLink;
    private String connectionString;

    @Before
    public void setup() {
        this.connectionString = mongoContainer.getConnectionString();
    }

    @After
    public void cleanup() throws Exception {
        dataLink.close();
        dataLink = null;
    }

    @Test
    public void should_return_same_link() {
        dataLink = new MongoDbDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released() {
        dataLink = new MongoDbDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoClient client2 = dataLink.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_return_collections_when_listResources() {
        dataLink = new MongoDbDataLink(mongoDataLinkConf("mongo", connectionString));

        MongoClient client1 = dataLink.getClient();
        MongoDatabase db1 = client1.getDatabase("test");
        MongoDatabase db2 = client1.getDatabase("test2");

        db1.createCollection("col1");
        db1.createCollection("col2");
        db2.createCollection("col3");

        assertThat(dataLink.listResources()).contains(
                new Resource("collection", "col1"),
                new Resource("collection", "col2"),
                new Resource("collection", "col3")
        );
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