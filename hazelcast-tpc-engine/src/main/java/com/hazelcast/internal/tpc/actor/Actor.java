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

package com.hazelcast.internal.tpc.actor;

import com.hazelcast.internal.tpc.Reactor;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

public abstract class Actor implements Runnable {

    public final static int DEFAULT_MAILBOX_CAPACITY = 512;

    private final MpscArrayQueue mailbox;
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private Reactor reactor;
    private final LocalActorRef handle = new LocalActorRef(this);

    public Actor() {
        this(DEFAULT_MAILBOX_CAPACITY);
    }

    public Actor(int mailboxCapacity) {
        this.mailbox = new MpscArrayQueue(mailboxCapacity);
    }

    /**
     * Returns the handle of the actor.
     *
     * @return the handle of the actor.
     */
    public LocalActorRef handle() {
        return handle;
    }

    /**
     * Returns the {@link Reactor} this actor belongs to.
     *
     * @return the Reactor this actor belongs to. If the actor hasn't been activated yet,
     * <code>null</code> is returned.
     */
    public Reactor reactor() {
        return reactor;
    }

    /**
     * Activates the Actor on the given reactor.
     * <p>
     * This method is not thread-safe.
     * <p>
     * This method should only be called once.
     *
     * @param reactor the Reactor this actor belongs to.
     * @throws IllegalStateException when the actor is already activated.
     * @throws NullPointerException  when reactor is <code>null</code>.
     */
    public void activate(Reactor reactor) {
        checkNotNull(reactor);

        if (this.reactor != null) {
            throw new IllegalStateException("Can't activate an already activated actor");
        }
        this.reactor = reactor;
    }

    void send(Object msg) {
        //todo: we need to deal with overload.
        mailbox.offer(msg);

        if (!scheduled.get() && scheduled.compareAndSet(false, true)) {
            reactor.offer(this);
        }
    }

    @Override
    public void run() {
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
                reactor.offer(this);
            }
        } else {
            reactor.offer(this);
        }
    }

    public abstract void process(Object msg);
}
