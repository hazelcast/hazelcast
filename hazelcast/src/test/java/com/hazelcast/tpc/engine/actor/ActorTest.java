package com.hazelcast.tpc.engine.actor;



import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.nio.NioEventloop;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ActorTest {

    private NioEventloop eventloop;

    @Before
    public void before(){
        eventloop = new NioEventloop();
        eventloop.start();
    }

    @Test
    public void test_receiveMsg(){
        CountDownLatch executed = new CountDownLatch(1);
        AtomicReference msgRef = new AtomicReference();
        Actor actor = new Actor() {
            @Override
            public void process(Object msg) {
                msgRef.set(msg);
                executed.countDown();
            }
        };
        ActorHandle handle = actor.getHandle();
        actor.activate(eventloop);
        String msg = "Message";
        handle.send(msg);
        assertOpenEventually(executed);
        assertEquals(msg, msgRef.get());
    }
}
