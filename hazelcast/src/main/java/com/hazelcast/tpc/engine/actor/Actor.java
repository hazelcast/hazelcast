/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    public Actor() {
        this(DEFAULT_MAILBOX_CAPACITY);
    }

    public Actor(int mailboxCapacity) {
        this.mailbox = new MpscArrayQueue(mailboxCapacity);
    }

    void send(Object msg) {
        //todo: we need to deal with overload.
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

            if (scheduled.compareAndSet(false, true)) {
                eventloop.execute(this);
            }
        } else {
            eventloop.execute(this);
        }
    }

    public abstract void process(Object msg);
}
