package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientListenersTest extends HazelcastTestSupport {

    HazelcastInstance client;

    @Before
    public void setup() {
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addPortableFactory(5, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 6) {
                    return new ClientIssueTest.SamplePortable();
                }
                return null;
            }
        });

        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(config);
    }

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testEntryListener_withPortableNotRegisteredInNode() throws Exception {
        final IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);

        map.put(1, new ClientIssueTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testItemListener_withPortableNotRegisteredInNode() throws Exception {
        final IQueue<Object> queue = client.getQueue(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        queue.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        queue.offer(new ClientIssueTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testSetListener_withPortableNotRegisteredInNode() throws Exception {
        final ISet<Object> set = client.getSet(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        set.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        set.add(new ClientIssueTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testListListener_withPortableNotRegisteredInNode() throws Exception {
        final IList<Object> list = client.getList(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        list.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        list.add(new ClientIssueTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testTopic_withPortableNotRegisteredInNode() throws Exception {
        final ITopic<Object> topic = client.getTopic(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        topic.addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {
                latch.countDown();
            }
        });

        topic.publish(new ClientIssueTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

}
