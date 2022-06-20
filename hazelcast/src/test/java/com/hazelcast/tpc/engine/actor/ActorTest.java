package com.hazelcast.tpc.engine.actor;


import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ActorTest {

    private NioEventloop eventloop;

    @Before
    public void before() {
        eventloop = new NioEventloop();
        eventloop.start();
    }

    @After
    public void after() throws InterruptedException {
        if (eventloop != null) {
            eventloop.shutdown();
            assertTrue(eventloop.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_activate_whenAlreadyActivated() {
        Actor actor = new Actor() {
            @Override
            public void process(Object msg) {
            }
        };
        actor.activate(eventloop);
        actor.activate(eventloop);
    }

    @Test
    public void test_receiveMsg() {
        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference msgRef = new AtomicReference();
        Actor actor = new Actor() {
            @Override
            public void process(Object msg) {
                msgRef.set(msg);
                executed.countDown();
            }
        };
        ActorRef ref = actor.handle();
        actor.activate(eventloop);
        String msg = "Message";
        ref.send(msg);
        assertOpenEventually(executed);
        assertEquals(msg, msgRef.get());
    }
}
