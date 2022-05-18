package com.hazelcast.tpc.engine.actor;

import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.EventloopTask;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Actor implements EventloopTask {

    public final static int DEFAULT_MAILBOX_CAPACITY = 512;

    private final MpscArrayQueue mailbox;

    private final AtomicBoolean scheduled = new AtomicBoolean();
    private Eventloop eventloop;
    private final LocalActorHandle handle = new LocalActorHandle(this);

    public LocalActorHandle getHandle() {
        return handle;
    }

    public Eventloop getEventloop() {
        return eventloop;
    }

    public void activate(Eventloop eventloop) {
        this.eventloop = eventloop;
    }

    public Actor(){
        this(DEFAULT_MAILBOX_CAPACITY);
    }

    public Actor(int mailboxCapacity){
         mailbox = new MpscArrayQueue(mailboxCapacity);
    }

    void send(Object msg) {
        mailbox.offer(msg);

        if (!scheduled.get() && scheduled.compareAndSet(false, true)) {
            eventloop.execute(this);
        }
    }

    @Override
    public void run() throws Exception {
        Object msg = mailbox.poll();
        if (msg != null) {
            try {
                process(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        unschedule();
    }

    private void unschedule() {
        if (mailbox.isEmpty()) {
            scheduled.set(false);

            if (mailbox.isEmpty()) {
                return;
            }
        }

        eventloop.execute(this);
    }

    public abstract void process(Object msg);
}
