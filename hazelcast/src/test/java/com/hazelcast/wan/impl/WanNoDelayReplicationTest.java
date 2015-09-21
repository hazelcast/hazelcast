package com.hazelcast.wan.impl;

import com.hazelcast.map.impl.wan.MapReplicationUpdate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.wan.ReplicationEventObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class WanNoDelayReplicationTest extends HazelcastTestSupport {

    @Test
    public void interruptedReplicationShouldNotDropAnEvent() throws InterruptedException {
        WanNoDelayReplication replication = spy(new WanNoDelayReplication());
        doThrow(InterruptedException.class).when(replication).getConnection();

        ReplicationEventObject eventObject = new MapReplicationUpdate();
        replication.publishReplicationEvent("service", eventObject);
        replication.run();
        replication.shutdown();

        assertEquals(0, replication.getEventQueue().size());
        assertEquals(1, replication.getFailureQ().size());
        assertSame(eventObject, replication.getFailureQ().get(0).getEventObject());
    }

    @Test
    public void interruptedOnAnEventShouldWorkNicely() {
        WanNoDelayReplication replication = new WanNoDelayReplication();

        // This will make the replication wait for an event
        final Thread thread = new Thread(replication);
        thread.start();

        // Wait just a little bit to make sure we are where we want
        sleepMillis(20);

        // Then interrupt the thread
        thread.interrupt();

        // The thread should stop nicely without error
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertFalse(thread.isAlive());
            }
        }, 20);
    }
}
