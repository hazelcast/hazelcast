package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.util.counters.SwCounter;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.internal.networking.nio.SendQueue.State.BLOCKED;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_DATA_ONLY;
import static com.hazelcast.internal.networking.nio.SendQueue.State.SCHEDULED_WITH_TASK;
import static com.hazelcast.internal.networking.nio.SendQueue.State.UNSCHEDULED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SendQueueTest {

    private SendQueue q;
    private SwCounter bytesWritten;

    @Before
    public void before() {
        bytesWritten = SwCounter.newSwCounter();
        q = new SendQueue(bytesWritten);
    }

    @Test
    public void constructor() {
        assertEquals(BLOCKED, q.state());
    }

    // ======================= offer ===============

    @Test
    public void offer_whenNewQueue() {
        boolean schedule = q.enque(newFrame());

        // should not be scheduled since pipeline is blocked.
        assertFalse(schedule);
        assertEquals(BLOCKED, q.state());
    }

    @Test
    public void offer_whenBlocked() {
        q.enque(newFrame());
        q.get();
        q.block();

        boolean schedule = q.enque(newFrame());

        // should not be scheduled since pipeline is blocked.
        assertFalse(schedule);
        assertEquals(BLOCKED, q.state());
    }

    @Test
    public void offer_whenAlreadyScheduled() {
        q.enque(newFrame());
        q.prepare();
        q.get();
        q.tryUnschedule();
        q.enque(newFrame());

        boolean schedule = q.enque(newFrame());

        // should not be scheduled since pipeline is blocked.
        assertFalse(schedule);
        assertEquals(SCHEDULED_DATA_ONLY, q.state());
    }

    @Test
    public void offer_whenUnscheduled() {
        q.enque(newFrame());
        q.prepare();
        q.get();
        System.out.println(q.tryUnschedule());

        OutboundFrame frame = newFrame();
        boolean schedule = q.enque(frame);

        assertTrue(schedule);
        assertEquals(SCHEDULED_DATA_ONLY, q.state());
    }


    @Test
    public void tmp() {
        OutboundFrame frame = newFrame();
        q.enque(frame);
        assertEquals(BLOCKED, q.state());
        q.prepare();
        assertEquals(BLOCKED, q.state());
        OutboundFrame f = q.get();
        assertSame(frame,f);


        System.out.println(q.tryUnschedule());
        assertEquals(UNSCHEDULED, q.state());

        OutboundFrame f2 = newFrame();
        boolean schedule = q.enque(f2);

        assertTrue(schedule);
        assertEquals(SCHEDULED_DATA_ONLY, q.state());
    }

    // ======================= execute ===============

    @Test
    public void execute_whenNewQueue() {
        Runnable task = mock(Runnable.class);
        boolean wakeup = q.enque(task);

        assertTrue(wakeup);
        assertEquals(SCHEDULED_WITH_TASK, q.state());
        verifyZeroInteractions(task);
    }

    @Test
    public void execute_whenBlocked() {
        q.enque(newFrame());
        q.get();
        q.block();

        Runnable task = mock(Runnable.class);
        boolean schedule = q.enque(task);

        assertTrue(schedule);
        assertEquals(SCHEDULED_WITH_TASK, q.state());
        verifyZeroInteractions(task);
    }

    @Test
    public void execute_whenScheduled() {
        q.enque(() -> {
        });

        OutboundFrame frame = newFrame();
        q.enque(frame);

        Runnable task = mock(Runnable.class);
        boolean schedule = q.enque(task);

        assertFalse(schedule);
        assertEquals(SCHEDULED_WITH_TASK, q.state());
        verifyZeroInteractions(task);
    }

    @Test
    public void execute_whenUnscheduled() {
        q.enque(() -> { });
        q.prepare();
        q.get();
        q.tryUnschedule();

        boolean schedule = q.enque(() -> {
        });

        assertTrue(schedule);
        assertEquals(SCHEDULED_WITH_TASK, q.state());
    }

    // ======================= tryUnschedule ===============

    // ======================= poll ===============

    @Test(expected = IllegalStateException.class)
    public void block_whenUnscheduled() {
        q.enque(() -> { });
        q.prepare();
        q.get();
        q.tryUnschedule();
        q.block();
    }

    @Test
    public void block_whenScheduled() {
        q.enque(newFrame());
        q.get();

        q.block();

        assertEquals(BLOCKED, q.state());
    }

    @Test
    public void block_whenBlocked() {
        q.enque(newFrame());
        q.get();
        q.block();

        q.block();

        assertEquals(BLOCKED, q.state());
    }

    // ======================= poll ===============

    @Test
    public void get_whenBlocked() {
        OutboundFrame f1 = newFrame();
        OutboundFrame f2 = newFrame();
        q.enque(f1);
        q.enque(f2);
        q.prepare();
        q.block();

        assertEquals(f1, q.get());
        assertEquals(f2, q.get());
        assertNull(q.get());
        assertEquals(BLOCKED, q.state());
    }

    @Test
    public void get_whenScheduledAndEmpty() {
        q.enque(() -> {
        });

        SendQueue.Node node = q.putStack.get();
        OutboundFrame f = q.get();
        assertNull(f);
        assertSame(node, q.putStack.get());
    }

//    // we can't poll if not scheduled.
//    @Test(expected = IllegalStateException.class)
//    public void get_whenUnscheduled() {
//        q = new SendQueue();
//        q.execute(() -> {
//        });
//        q.tryUnschedule();
//
//        SendQueue.Node node = q.putStack.get();
//        OutboundFrame f = q.get();
//        assertNull(f);
//        assertSame(node, q.putStack.get());
//    }

    private static OutboundFrame newFrame() {
        return newFrame(100);
    }

    private static OutboundFrame newFrame(int length) {
        OutboundFrame frame = mock(OutboundFrame.class);
        when(frame.isUrgent()).thenReturn(false);
        when(frame.getFrameLength()).thenReturn(length);
        return frame;
    }
}
