package com.hazelcast.client.impl.querycache;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContextTest.QuerySchedulerRepetitionTask;
import com.hazelcast.map.impl.querycache.NodeQueryCacheContextTest.QuerySchedulerTask;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.QueryCacheScheduler;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheContextTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    private QueryCacheContext context;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        factory.newHazelcastInstance();

        HazelcastInstance hz = factory.newHazelcastClient();
        ClientMapProxy proxy = (ClientMapProxy) hz.getMap("test");
        context = proxy.getQueryContext();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDestroy() {
        context.destroy();
    }

    @Test
    public void testGetMemberList() {
        Collection<Member> memberList = context.getMemberList();

        assertNotNull(memberList);
        assertEquals(1, memberList.size());
    }

    @Test
    public void testPartitionId() {
        int partitionId = context.getPartitionId("myKey");

        assertTrue(partitionId >= 0);
    }

    @Test
    public void testGetQueryCacheScheduler() {
        QueryCacheScheduler scheduler = context.getQueryCacheScheduler();
        assertNotNull(scheduler);

        final QuerySchedulerTask task = new QuerySchedulerTask();
        scheduler.execute(task);

        final QuerySchedulerRepetitionTask repetitionTask = new QuerySchedulerRepetitionTask();
        scheduler.scheduleWithRepetition(repetitionTask, 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(task.executed);
                assertTrue(repetitionTask.counter.get() > 1);
            }
        });

        scheduler.shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetPublisherContext() {
        context.getPublisherContext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetThisNodesAddress() {
        context.getThisNodesAddress();
    }
}
