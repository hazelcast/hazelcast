/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorBuilder;
import com.hazelcast.internal.tpcengine.net.AcceptRequest;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocketBuilder;
import com.hazelcast.internal.tpcengine.net.AsyncSocketBuilder;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * io_uring implementation of the {@link Eventloop}.
 *
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 */
public class IOUringReactor extends Reactor {

    //todo: Litter; we need to come up with better solution.
    protected final Set<AutoCloseable> closeables = new CopyOnWriteArraySet<>();
    private final EventFd eventFd;

    public IOUringReactor() {
        this(new IOUringReactorBuilder());
    }

    public IOUringReactor(IOUringReactorBuilder builder) {
        super(builder);
        this.eventFd = ((IOUringEventloop) eventloop()).eventfd;
    }

    /**
     * Registers an AutoCloseable on this Reactor.
     * <p>
     * Registered closeable are automatically closed when the Reactor shuts down.
     * Some examples: AsyncSocket and AsyncServerSocket.
     * <p>
     * If the Eventloop isn't in the running state, false is returned.
     * <p>
     * This method is thread-safe.
     *
     * @param closeable the AutoCloseable to register
     * @return true if the closeable was successfully register, false otherwise.
     * @throws NullPointerException if closeable is null.
     */
    public boolean registerCloseable(AutoCloseable closeable) {
        checkNotNull(closeable, "closeable");

        if (state != State.RUNNING) {
            return false;
        }

        closeables.add(closeable);

        if (state != State.RUNNING) {
            closeables.remove(closeable);
            return false;
        }

        return true;
    }

    /**
     * Deregisters an AutoCloseable from this Eventloop.
     * <p>
     * This method is thread-safe.
     * <p>
     * This method can be called no matter the state of the Eventloop.
     *
     * @param closeable the AutoCloseable to deregister.
     */
    public void deregisterCloseable(AutoCloseable closeable) {
        closeables.remove(checkNotNull(closeable, "closeable"));
    }

    @Override
    protected Eventloop newEventloop(ReactorBuilder builder) {
        return new IOUringEventloop(this, (IOUringReactorBuilder) builder);
    }

    @Override
    public AsyncSocketBuilder newAsyncSocketBuilder() {
        verifyRunning();

        return new IOUringAsyncSocketBuilder(this, null);
    }

    @Override
    public AsyncSocketBuilder newAsyncSocketBuilder(AcceptRequest acceptRequest) {
        verifyRunning();

        IOUringAcceptRequest ioUringAcceptRequest
                = checkInstanceOf(IOUringAcceptRequest.class, acceptRequest, "acceptRequest");
        return new IOUringAsyncSocketBuilder(this, ioUringAcceptRequest);
    }

    @Override
    public AsyncServerSocketBuilder newAsyncServerSocketBuilder() {
        verifyRunning();

        return new IOUringAsyncServerSocketBuilder(this);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            eventFd.write(1L);
        }
    }
}

